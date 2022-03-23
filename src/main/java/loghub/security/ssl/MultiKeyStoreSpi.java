package loghub.security.ssl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AlgorithmParameters;
import java.security.DomainLoadStoreParameter;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStore.LoadStoreParameter;
import java.security.KeyStoreException;
import java.security.KeyStoreSpi;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.Helpers;
import loghub.security.ssl.MultiKeyStoreProvider.SubKeyStore;

public class MultiKeyStoreSpi extends KeyStoreSpi {

    private static final Logger logger = LogManager.getLogger();

    private static final Map<String, KeyFactory> kfmap = new ConcurrentHashMap<>();
    private static final CertificateFactory cf;
    private static final MessageDigest digest;
    static {
        try {
            cf = CertificateFactory.getInstance("X.509");
            digest = MessageDigest.getInstance("MD5");
        } catch (CertificateException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing security algorithms", e);
        }
    }
    private static final String DEFAULT_ALIAS = "__default_alias__";
    private static final String MIME_TYPE = "content-type";
    public static final KeyStore.ProtectionParameter EMPTYPROTECTION = new KeyStore.PasswordProtection(new char[] {});

    private static final Pattern MARKERS;
    static {
        String epk = "(?<epk>ENCRYPTED PRIVATE KEY)"; //encrypted PKCS#8 key
        String privateKey = "(?<prk>PRIVATE KEY)";    //not encrypted PKCS#8
        String rsakey = "(?<rprk>RSA PRIVATE KEY)";
        String pubKey = "(?<puk>PUBLIC KEY)";
        String cert = "(?<cert>CERTIFICATE)";
        String begin = "(?<begin>-+BEGIN .*-+)";
        String end = String.format("(?<end>-+END (?:%s|%s|%s|%s|%s)-+)", privateKey, rsakey, pubKey, cert, epk);
        MARKERS = Pattern.compile(String.format("(?:%s)|(?:%s)|.*?", begin, end));
    }
    private static final Base64.Decoder decoder = Base64.getDecoder();
    private static final Base64.Encoder encoder = Base64.getEncoder();

    private final List<KeyStore> stores = new ArrayList<>();

    public MultiKeyStoreSpi() {
        try {
            // An empty initial trust store, for loaded PEM
            KeyStore first = KeyStore.getInstance("JKS");
            first.load(null, null);
            stores.add(first);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new IllegalStateException("Missing security algorithms", e);
        }
    }

    @Override
    public Key engineGetKey(String alias, char[] password) throws NoSuchAlgorithmException, UnrecoverableKeyException {
        for (KeyStore ks: stores) {
            try {
                Key val = ks.getKey(alias, password);
                if (val != null) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return null;
    }

    @Override
    public Certificate[] engineGetCertificateChain(String alias) {
        for (KeyStore ks: stores) {
            try {
                Certificate[] val = ks.getCertificateChain(alias);
                if (val != null) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return new Certificate[] {};
    }

    @Override
    public Certificate engineGetCertificate(String alias) {
        for (KeyStore ks: stores) {
            try {
                Certificate val = ks.getCertificate(alias);
                if (val != null) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return null;
    }

    @Override
    public Date engineGetCreationDate(String alias) {
        for (KeyStore ks: stores) {
            try {
                Date val = ks.getCreationDate(alias);
                if (val != null) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return null;
    }

    @Override
    public void engineSetKeyEntry(String alias, Key key, char[] password, Certificate[] chain) throws KeyStoreException {
        throw new UnsupportedOperationException("Read-only key store");
    }

    @Override
    public void engineSetKeyEntry(String alias, byte[] key, Certificate[] chain) throws KeyStoreException {
        throw new UnsupportedOperationException("Read-only key store");
    }

    @Override
    public void engineSetCertificateEntry(String alias, Certificate cert) throws KeyStoreException {
        throw new UnsupportedOperationException("Read-only key store");
    }

    @Override
    public void engineDeleteEntry(String alias) throws KeyStoreException {
        throw new UnsupportedOperationException("Read-only key store");
    }

    /**
     * Find the first not empty Keystore extracted from the iterator
     * @param iter
     * @return a Keystore or null if no more iterator to check
     */
    private KeyStore findNonEmpty(Iterator<KeyStore> iter) {
        KeyStore totry = null;
        // The aliases enumerator is not usable (empty or null), find the next one
        // Find the next non empty KeyStore
        int kssize = 0;
        while (iter.hasNext()) {
            totry = iter.next();
            try {
                kssize = totry.size();
                if (kssize != 0) {
                    break;
                } else {
                    totry = null;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return totry;
    }

    private Enumeration<String> findNext(Enumeration<String> enumerator, Iterator<KeyStore> iter) {
        while (enumerator == null || ! enumerator.hasMoreElements()) {
            KeyStore cur = findNonEmpty(iter);
            // The last keystore found was empty or no more to try, keystore enumeration is finished
            if (cur == null) {
                enumerator = null;
                break;
            }
            try {
                enumerator = cur.aliases();
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return enumerator;
    }

    @Override
    public Enumeration<String> engineAliases() {
        Iterator<KeyStore> iter = stores.iterator();

        return new Enumeration<String>(){
            private Enumeration<String> enumerator = findNext(null, iter);
            @Override
            public boolean hasMoreElements() {
                enumerator = findNext(enumerator, iter);
                // If was unable to find a valid new enumerator, enumeration is finished
                return enumerator != null;
            }

            @Override
            public String nextElement() {
                if (enumerator == null) {
                    throw new NoSuchElementException();
                } else {
                    return enumerator.nextElement();
                }
            }
        };
    }

    @Override
    public boolean engineContainsAlias(String alias) {
        for (KeyStore ks: stores) {
            try {
                boolean val = ks.containsAlias(alias);
                if (val) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return false;
    }

    @Override
    public int engineSize() {
        int size = 0;
        for (KeyStore ks: stores) {
            try {
                size += ks.size();
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return size;
    }

    @Override
    public boolean engineIsKeyEntry(String alias) {
        for (KeyStore ks: stores) {
            try {
                boolean val = ks.isKeyEntry(alias);
                if (val) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return false;
    }

    @Override
    public boolean engineIsCertificateEntry(String alias) {
        for (KeyStore ks: stores) {
            try {
                boolean val = ks.isCertificateEntry(alias);
                if (val) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return false;
    }

    @Override
    public String engineGetCertificateAlias(Certificate cert) {
        for (KeyStore ks: stores) {
            try {
                String val = ks.getCertificateAlias(cert);
                if (val != null) {
                    return val;
                }
            } catch (KeyStoreException e) {
                // This keystore is broken, just skip it
            }
        }
        return null;
    }

    @Override
    public void engineStore(OutputStream stream, char[] password) throws IOException, NoSuchAlgorithmException, CertificateException {
        throw new UnsupportedOperationException("Non persistent key store");
    }

    @Override
    public void engineLoad(InputStream stream, char[] password) throws IOException, NoSuchAlgorithmException, CertificateException {
        throw new UnsupportedOperationException("Non persistent key store");
    }

    @Override
    public void engineLoad(LoadStoreParameter param) {
        if (param instanceof SubKeyStore) {
            SubKeyStore subparams = (SubKeyStore) param;
            for (String i: subparams.substores) {
                try {
                    addStore(i);
                } catch (RuntimeException | IOException | GeneralSecurityException ex) {
                    logger.error("Unable to load keystore {}: {}", i, Helpers.resolveThrowableException(ex));
                    logger.catching(Level.DEBUG, ex);
                }
            }
        } else {
            throw new UnsupportedOperationException("Needs a SubKeyStore param, not a " + param.getClass().getCanonicalName());
        }
    }

    private void addStore(String path) throws GeneralSecurityException, IOException {
        if ("system".equals(path)) {
            logger.debug("Loading OS dependent certificates");
            String operatingSystem = System.getProperty("os.name", "");
            String[] systemStores = new String[] {};
            
            if (operatingSystem.startsWith("Mac")) {
                systemStores = new String[] {"KeychainStore"};
            } else if (operatingSystem.startsWith("Windows")){
                systemStores = new String[] {"Windows-MY", "Windows-ROOT"};
            } else if (operatingSystem.startsWith("Linux")) {
                // Paths where linux might store certs
                for (String certsPath: new String[] {
                        "/etc/ssl/certs/ca-certificates.crt",
                        "/etc/pki/tls/cert.pem",
                        "/etc/ssl/cert.pem",
                        "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt",
                        "/usr/share/pki/ca-trust-legacy/ca-bundle.legacy.default.crt",
                }) {
                    if (Files.isReadable(Paths.get(certsPath))) {
                        loadPem(Paths.get(certsPath).toUri(), Collections.emptyMap());
                        break;
                    }
                }
            }
            if (systemStores.length > 0) {
                for (String n: systemStores) {
                    KeyStore ks = KeyStore.getInstance(n);
                    ks.load(null, "".toCharArray());
                    stores.add(ks);
                }
            }
        } else if ("default".equals(path)) {
            logger.debug("Loading current JVM default certificates");
            String[] paths = new String[] {
                    System.getProperty("java.home") + File.separator + "lib" + File.separator + "security" + File.separator + "jssecacerts",
                    System.getProperty("java.home") + File.separator + "lib" + File.separator + "security" + File.separator + "cacerts"
            };
            for (String storePathName: paths) {
                Path storePath = Paths.get(storePathName);
                if (Files.exists(storePath)) {
                    loadKeystore("JKS", storePath.toUri(), Collections.singletonMap("password", "changeit"));
                    break;
                }
            }
        } else {
            URI pathURI = Helpers.FileUri(path);
            QueryStringDecoder qsd = new QueryStringDecoder(pathURI);
            // Stream to only keep the last value of any parameter
            Map<String, Object> fileParams = qsd.parameters().entrySet().stream()
                                                .filter(e -> e.getValue() != null)
                                                .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().get(e.getValue().size() -1)))
                                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            fileParams.put(DEFAULT_ALIAS, encoder.encodeToString(digest.digest(path.getBytes())));
            if (pathURI.getPath().toLowerCase().endsWith(".policy")) {
                loadDomain(pathURI, fileParams);
            } else if (Paths.get(pathURI.getPath()).endsWith("cacerts")) {
                fileParams.computeIfAbsent("password", (k) -> "changeit");
                loadKeystore("JKS", pathURI, fileParams);
            } else {
                String mimetype = resolveMimeType(pathURI, fileParams);
                switch(mimetype) {
                case "application/x-pkcs12":
                    loadKeystore("PKCS12", pathURI, fileParams);
                    break;
                case "application/x-java-keystore":
                    loadKeystore("JKS", pathURI, fileParams);
                    break;
                case "application/x-java-jce-keystore":
                    loadKeystore("JCEKS", pathURI, fileParams);
                    break;
                case "application/x-java-bc-keystore":
                    loadKeystore("BKS", pathURI, fileParams);
                    break;
                case "application/x-java-bc-uber-keystore":
                    loadKeystore("Keystore.UBER", pathURI, fileParams);
                    break;
                case "application/x-pem-file":
                    loadPem(pathURI, fileParams);
                    break;
                case "application/pkix-cert":
                    loadCert(pathURI, fileParams);
                    break;
                default:
                    throw new NoSuchAlgorithmException("Not managed file type: " + mimetype);
                }
            }
        }
    }

    private String resolveMimeType(URI path, Map<String, Object> params) {
        if (params.containsKey(MIME_TYPE)) {
            return params.get(MIME_TYPE).toString();
        } else {
            return Helpers.getMimeType(path.getPath());
        }
    }

    private Map.Entry<String, KeyStore.ProtectionParameter> mapProtectionParameter(Map.Entry<String, Object> e) {
        return new AbstractMap.SimpleImmutableEntry<>(
                e.getKey(),
                new KeyStore.PasswordProtection(e.getValue().toString().toCharArray())
        );
    }

    private void loadDomain(URI path, Map<String, Object> fileParams)
            throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
        logger.debug("Loading domain store {}", path);
        Map<String, KeyStore.ProtectionParameter> domainParameters = fileParams.entrySet().stream()
                                                         .map(this::mapProtectionParameter)
                                                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        DomainLoadStoreParameter params = new DomainLoadStoreParameter(path, domainParameters);
        KeyStore ks = KeyStore.getInstance("DKS");
        ks.load(params);
        stores.add(ks);
    }

    private void loadCert(URI path, Map<String, Object> fileParams)
            throws CertificateException, IOException, KeyStoreException {
        logger.debug("Loading binary certificate {}", path);
        try (InputStream fis = path.toURL().openStream()){
            Certificate cert = cf.generateCertificate(fis);
            addEntry(Collections.singletonList(cert), null, fileParams);
        }
    }

    private void loadKeystore(String type, URI path, Map<String, Object> fileParams) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        logger.debug("Loading keystore {} as {}", path, type);
        KeyStore ks = KeyStore.getInstance(type);
        String password = (String) fileParams.getOrDefault("password", "");
        try (InputStream is = path.toURL().openStream()) {
            ks.load(is, password.toCharArray());
            stores.add(ks);
        }
    }

    /**
     * Two kind of PEM files are expected:
     * <ul>
     *     <li>A list of certificates, they are imported as separated certificates entries.</li>
     *     <li>A private key, and one or more certificates. It’s used a certificate chain and are added as a single entry.</li>
     * </ul>
     * If no alias is provided for the secret key, the first value of the dn for the leaf certificate will be used.
     * @param path the URI to the pem file
     * @param fileParams some parameters, it can contain the alias that will be used for the secret key.
     * @throws CertificateException
     * @throws IOException
     * @throws InvalidKeySpecException
     * @throws KeyStoreException
     */
    private void loadPem(URI path, Map<String, Object> fileParams) throws IOException, GeneralSecurityException
    {
        logger.debug("Loading PEM {}", path);
        List<Certificate> certs = new ArrayList<>(1);
        PrivateKey key = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(path.toURL().openStream(), StandardCharsets.UTF_8))) {
            String line;
            StringBuilder buffer = new StringBuilder();
            while ((line = br.readLine()) != null) {
                Matcher matcher = MARKERS.matcher(line);
                matcher.matches();
                if (matcher.group("begin") != null) {
                    buffer.setLength(0);
                } else if (matcher.group("end") != null) {
                    byte[] content = decoder.decode(buffer.toString());
                    digest.reset();
                    if (matcher.group("cert") != null) {
                        certs.add(cf.generateCertificate(new ByteArrayInputStream(content)));
                    } else if (matcher.group("prk") != null ||
                            matcher.group("rprk") != null ||
                            matcher.group("epk") != null){
                        if (key != null) {
                            throw new IllegalArgumentException("Multiple key in a PEM");
                        }
                        if (matcher.group("rprk") != null) {
                            content = convertPkcs1(content);
                        }
                        if (matcher.group("epk") != null) {
                            content = decrypteEncryptedPkcs8(content, fileParams.getOrDefault("password", "").toString());
                        }
                        PKCS8Codec codec = new PKCS8Codec(ByteBuffer.wrap(content));
                        codec.read();
                        PKCS8EncodedKeySpec keyspec = new PKCS8EncodedKeySpec(content);
                        key = kfmap.computeIfAbsent(codec.getAlgoOidString(), this::resolveFactory).generatePrivate(keyspec);
                    } else {
                        throw new IllegalArgumentException("Unknown entry type in PEM");
                    }
                } else {
                    buffer.append(line);
                }
            }
            addEntry(certs, key, fileParams);
        }
    }

    private KeyFactory resolveFactory(String protocol) {
        try {
            return KeyFactory.getInstance(protocol);
        } catch (NoSuchAlgorithmException ex) {
            throw new UndeclaredThrowableException(ex);
        }
    }

    /**
     * Wrap PKCS#1 bytes as a PCKS1#8 buffer by prefixing with the right header
     * @param pkcs1Bytes
     * @return
     */
    private byte[] convertPkcs1(byte[] pkcs1Bytes) {
        int pkcs1Length = pkcs1Bytes.length;
        int totalLength = pkcs1Length + 22;
        int bufferLength = totalLength + 4;
        byte[] pkcs8Header = new byte[] {
                0x30, (byte) 0x82, (byte) ((totalLength >> 8) & 0xff), (byte) (totalLength & 0xff), // Sequence + total length
                0x2, 0x1, 0x0, // Integer (0)
                0x30, 0xD, 0x6, 0x9, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7, 0xD, 0x1, 0x1, 0x1, 0x5, 0x0, // Sequence: 1.2.840.113549.1.1.1, NULL
                0x4, (byte) 0x82, (byte) ((pkcs1Length >> 8) & 0xff), (byte) (pkcs1Length & 0xff) // Octet string + length
        };
        byte[] pkcs8bytes = new byte[bufferLength];
        ByteBuffer pkcs8buffer = ByteBuffer.wrap(pkcs8bytes);
        pkcs8buffer.put(pkcs8Header);
        pkcs8buffer.put(pkcs1Bytes);
        return pkcs8bytes;
    }

    private void addEntry(List<Certificate> certs, PrivateKey key, Map<String, Object> params) throws KeyStoreException {
        if (certs.size() == 0) {
            throw new IllegalArgumentException("No certificates to store");
        }
        String alias = (String) params.get("alias");
        if (alias == null) {
            Iterator<Certificate> iter = certs.iterator();
            Certificate leaf = null;
            while (iter.hasNext()) {
                Certificate cert = iter.next();
                if ("X.509".equals(cert.getType())) {
                    X509Certificate x509cert = (X509Certificate) cert;
                    if (x509cert.getBasicConstraints() == -1) {
                        // The leaf certificate
                        leaf = cert;
                        iter.remove();
                        break;
                    }
                }
            }
            if (leaf != null) {
                alias = resolveAlias(leaf, params);
                // Ensure that the leaf certificate is the first
                certs.add(0, leaf);
            }
        }
        if (alias == null) {
            alias = (String) params.get(DEFAULT_ALIAS);
        }
        if (key != null && stores.get(0).containsAlias(alias)) {
            // If object already seen, don't add it again
            logger.warn("Duplicate entry {}", alias);
        } else if (key != null) {
            KeyStore.PrivateKeyEntry entry = new KeyStore.PrivateKeyEntry(key, certs.stream().toArray(Certificate[]::new));
            stores.get(0).setEntry(alias, entry, EMPTYPROTECTION);
        } else {
            for (Certificate cert: certs) {
                String certAlias = resolveAlias(cert, params);
                KeyStore.TrustedCertificateEntry entry = new KeyStore.TrustedCertificateEntry(cert);
                stores.get(0).setEntry(certAlias, entry, null);
            }
        }
    }

    private String resolveAlias(Certificate cert, Map<String, Object> params) {
        String certalias = null;
        if ("X.509".equals(cert.getType())) {
            X509Certificate x509cert = (X509Certificate) cert;
            try {
                LdapName ldapDN = new LdapName(x509cert.getSubjectX500Principal().getName());
                // Order of values are inversed between LDAP and X.509, so get the last one.
                certalias = ldapDN.getRdn(ldapDN.size() - 1).getValue().toString();
            } catch (InvalidNameException e) {
            }
        }
        return  certalias;
    }

    private byte[] decrypteEncryptedPkcs8(byte[] epk, String password)
            throws IOException, GeneralSecurityException
    {
        EncryptedPrivateKeyInfo epki = new EncryptedPrivateKeyInfo(epk);

        String encAlg = epki.getAlgName();

        AlgorithmParameters encAlgParams = epki.getAlgParameters();
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
        SecretKeyFactory keyFact = SecretKeyFactory.getInstance(encAlg);
        SecretKey pbeKey = keyFact.generateSecret(pbeKeySpec);

        Cipher cipher = Cipher.getInstance(encAlg);

        cipher.init(Cipher.DECRYPT_MODE, pbeKey, encAlgParams);
        PKCS8EncodedKeySpec privateKeySpec = epki.getKeySpec(cipher);

        return privateKeySpec.getEncoded();
    }

}
