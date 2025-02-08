package loghub.security.ssl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import javax.security.auth.x500.X500Principal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.configuration.BeansPostProcess;
import loghub.configuration.ConfigException;
import lombok.Getter;
import lombok.Setter;

@BeansPostProcess(SslContextBuilder.BeansProcessor.class)
public class SslContextBuilder {

    public static class BeansProcessor extends BeansPostProcess.Processor {
        @Override
        public void process(Map<String, Method> beans) {
            try {
                beans.put("trusts", SslContextBuilder.class.getDeclaredMethod("setTrusts", Object[].class));
                beans.put("issuers", SslContextBuilder.class.getDeclaredMethod("setTrustedIssuers", Object[].class));
                beans.put("name", SslContextBuilder.class.getDeclaredMethod("setSslContextName", String.class));
            } catch (NoSuchMethodException ex) {
                throw new UnsupportedOperationException("Missing method", ex);
            }
        }
    }

    private static final SecureRandom DEFAULT_SECURERANDOM;
    static {
        try {
            String operatingSystem = System.getProperty("os.name", "");
            if (operatingSystem.startsWith("Windows")) {
                DEFAULT_SECURERANDOM = SecureRandom.getInstance("Windows-PRNG");
            } else {
                DEFAULT_SECURERANDOM = SecureRandom.getInstance("NativePRNGNonBlocking");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException("Default secure random not available", e);
        }
    }

    private static final KeyStore DEFAULT_KEYSTORE;
    static {
        try {
            DEFAULT_KEYSTORE = KeyStore.getInstance(KeyStore.getDefaultType());
            char[] pwdArray = "changeit".toCharArray();
            DEFAULT_KEYSTORE.load(null, pwdArray);
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException("Unusable default key store", e);
        }
    }
    private static final Logger logger = LogManager.getLogger();

    public static SslContextBuilder getBuilder() {
        return new SslContextBuilder();
    }

    public static SslContextBuilder getBuilder(Map<String, Object> properties) {
        return getBuilder(SslContextBuilder.class.getClassLoader(), properties);
    }

    /**
     * <p>Construct a {@link javax.net.ssl.SSLContext} using the given properties as a set of attributes that defines how it will be configure.</p>
     * <p>The possible key ares</p>
     * <ul>
     * <li><code>context</code>, The SSL protocol to use, as defined by {@link javax.net.ssl.SSLContext#getInstance(String protocol)}. Default to <code>TLS</code>.</li>
     * <li><code>providername</code>, A optional security provider for the {@link javax.net.ssl.SSLContext}.</li>
     * <li><code>providerclass</code>, A optional class that will used as a {@link java.security.Provider}, that will used to get the {@link javax.net.ssl.SSLContext}.</li>
     * <li><code>issuers</code>, a array of issuers DN that can be used to validate x.509 clients certificates.</li>
     * <li><code>clientAlias</code>, The alias of the private key used to authentify</li>
     * <li><code>ephemeralDHKeySize</code>, Can be used to override the <code>jdk.tls.ephemeralDHKeySiz</code> property. Default to 2048.</li>
     * <li><code>rejectClientInitiatedRenegotiation</code>, can be used to override the <code>jdk.tls.rejectClientInitiatedRenegotiatio</code> property. Default to true.</li>
     * <li><code>trusts</code>, a string array of trust source files or definitions.</li>
     * <li><code>issuers</code>, an string array of DN of valid issuers for client authentication.</li>
     * <li><code>securerandom</code>, The name of the {@link java.security.SecureRandom} that will be used, default to <code>NativePRNGNonBlocking</code>.</li>
     * </ul>
     * Either <code>providername</code> or <code>providerclass</code> can be used. If both are defined, <code>providername</code> will be used.
     * @param cl The class loader that will be used to find the {@link java.security.Provider} class if needed. It can be null.
     * @param properties a set of properties.
     * @return a SSL context built according to the given properties
     */
    public static SslContextBuilder getBuilder(ClassLoader cl, Map<String, Object> properties) {
        logger.debug("Configuring ssl context with {}", () -> properties);
        if (properties.containsKey("ephemeralDHKeySize")) {
            System.setProperty("jdk.tls.ephemeralDHKeySize", properties.get("ephemeralDHKeySize").toString());
        } else {
            System.setProperty("jdk.tls.ephemeralDHKeySize", "2048");
        }
        if (properties.containsKey("rejectClientInitiatedRenegotiation")) {
            System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", properties.get("rejectClientInitiatedRenegotiation").toString());
        } else {
            System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", "true");
        }
        SslContextBuilder builder = new SslContextBuilder();
        Optional.ofNullable(cl).ifPresent(s -> builder.classLoader = s);
        Optional.ofNullable(properties.get("context")).map(Object::toString).ifPresent(s -> builder.sslContextName = s);
        Optional.ofNullable(properties.get("providername")).map(Object::toString).ifPresent(s -> builder.sslProviderName = s);
        Optional.ofNullable(properties.get("providerclass")).map(Object::toString).ifPresent(s -> builder.sslProviderClass = s);
        Optional.ofNullable(properties.get("keymanageralgorithm")).map(Object::toString).ifPresent(s -> builder.keyManagerAlgorithm = s);
        Optional.ofNullable(properties.get("trustmanageralgorithm")).map(Object::toString).ifPresent(s -> builder.trustManagerAlgorithm = s);
        Optional.ofNullable(properties.get("securerandom")).map(Object::toString).ifPresent(builder::setSecureRandom);
        Optional.ofNullable(properties.get("trusts")).map(builder::getKeyStore).ifPresent(s -> builder.trusts = s);
        Optional.ofNullable(properties.get("clientAlias")).map(Object::toString).ifPresent(s -> builder.clientAlias = s);
        Optional.ofNullable(properties.get("clientSessionTimeout")).map(Number.class::cast).ifPresent(s -> builder.clientSessionTimeout = s.intValue());
        Optional.ofNullable(properties.get("clientSessionCacheSize")).map(Number.class::cast).ifPresent(s -> builder.clientSessionCacheSize = s.intValue());
        Optional.ofNullable(properties.get("serverSessionTimeout")).map(Number.class::cast).ifPresent(s -> builder.serverSessionTimeout = s.intValue());
        Optional.ofNullable(properties.get("serverSessionCacheSize")).map(Number.class::cast).ifPresent(s -> builder.serverSessionCacheSize = s.intValue());

        if (properties.containsKey("issuers") && properties.get("issuers") instanceof Object[]) {
            Object[] issuers = (Object[]) properties.get("issuers");
            builder.setTrustedIssuers(issuers);
            logger.debug("Will filter valid SSL client issuers as {}", builder.trustedIssuers);
        }
        return builder;
    }

    private interface GetInstanceWithProvider<T> {
        T getInstance(String name, Provider provider) throws NoSuchAlgorithmException;
    }

    private interface GetInstance<T> {
        T getInstance(String name) throws NoSuchAlgorithmException;
    }

    @Setter @Getter
    private String sslContextName = "TLSv1.2";
    @Setter
    private String sslProviderName = "";
    @Setter
    private String sslProviderClass = "";
    @Setter
    private String keyManagerAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
    @Setter
    private String trustManagerAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
    @Setter
    private ClassLoader classLoader = SslContextBuilder.class.getClassLoader();
    private KeyStore trusts = DEFAULT_KEYSTORE;
    private Set<Principal> trustedIssuers = null;
    @Setter @Getter
    private String clientAlias = null;
    private TrustManagerFactory trustManagerFactory = null;
    private KeyManagerFactory keyManagerFactory = null;
    @Getter
    private SecureRandom secureRandom = DEFAULT_SECURERANDOM;
    private Provider secureProvider = null;
    @Setter
    private int clientSessionTimeout = -1;
    @Setter
    private int clientSessionCacheSize = -1;
    @Setter
    private int serverSessionTimeout = -1;
    @Setter
    private int serverSessionCacheSize = -1;

    private SslContextBuilder() {
    }

    public SSLContext build() {
        try {
            SSLContext newCtxt;
            getSecureProvider();
            if (! sslProviderName.isEmpty()) {
                newCtxt = SSLContext.getInstance(sslContextName, sslProviderName);
            } else {
                newCtxt = doProvide(sslContextName, secureProvider, SSLContext::getInstance, SSLContext::getInstance);
            }
            KeyManager[] km;
            TrustManager[] tm;
            X509KeyManager kmtranslator;
            tm = getTrustManagerFactory().getTrustManagers();
            km = getKeyManagerFactory().getKeyManagers();
            X509ExtendedKeyManager origkm = (X509ExtendedKeyManager) km[0];
            kmtranslator = new DynamicKeyManager(origkm, trustedIssuers, clientAlias);

            newCtxt.init(new KeyManager[] {kmtranslator}, tm, secureRandom);

            if (clientSessionCacheSize >= 0) {
                newCtxt.getClientSessionContext().setSessionCacheSize(clientSessionCacheSize);
            }
            if (clientSessionTimeout >= 0) {
                newCtxt.getClientSessionContext().setSessionTimeout(clientSessionTimeout);
            }
            if (serverSessionCacheSize >= 0) {
                newCtxt.getServerSessionContext().setSessionCacheSize(serverSessionCacheSize);
            }
            if (serverSessionTimeout >= 0) {
                newCtxt.getServerSessionContext().setSessionTimeout(serverSessionTimeout);
            }
            return newCtxt;
        } catch (NoSuchProviderException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException |
                 UnrecoverableKeyException | ConfigException e) {
            throw new IllegalArgumentException("Failed to configure SSL context", e);
        }
    }

    public Provider getSecureProvider() {
        try {
            if (secureProvider == null && ! sslProviderClass.isEmpty()) {
                secureProvider = loadByName(classLoader, sslProviderClass);
            } else if (secureProvider == null && ! sslProviderName.isEmpty()) {
                secureProvider = SSLContext.getInstance(sslContextName, sslProviderName).getProvider();
            } else {
                secureProvider = SSLContext.getDefault().getProvider();
            }
            return secureProvider;
        } catch (NoSuchProviderException | NoSuchAlgorithmException ex) {
            throw new IllegalArgumentException("Unavailable provider", ex);
        }
    }

    public TrustManagerFactory getTrustManagerFactory()
            throws KeyStoreException, NoSuchAlgorithmException {
        getSecureProvider();
        if (trustManagerFactory == null) {
            TrustManagerFactory tmf = doProvide(trustManagerAlgorithm, secureProvider, TrustManagerFactory::getInstance, TrustManagerFactory::getInstance);
            tmf.init(trusts);
            trustManagerFactory = tmf;
        }
        return trustManagerFactory;
    }

    public KeyManagerFactory getKeyManagerFactory()
            throws KeyStoreException, NoSuchAlgorithmException,  UnrecoverableKeyException {
        getSecureProvider();
        if (keyManagerFactory == null) {
            keyManagerFactory = doProvide(keyManagerAlgorithm, secureProvider, KeyManagerFactory::getInstance, KeyManagerFactory::getInstance);
            keyManagerFactory.init(trusts, "".toCharArray());
        }
        return keyManagerFactory;
    }

    public void setTrusts(KeyStore keystore) {
        this.trusts = keystore;
    }

    public void setTrusts(Path keystore) {
        this.trusts = getKeyStore(keystore.toString());
    }

    public void setTrusts(Object[] trusts) {
        this.trusts = getKeyStore(trusts);
    }

    public void setTrustedIssuers(Object[] issuers) {
        this.trustedIssuers =  Arrays.stream(issuers)
                                     .filter(Objects::nonNull)
                                     .map(Object::toString)
                                     .map(this::resolvePrincipal)
                                     .filter(Objects::nonNull)
                                     .collect(Collectors.toSet());
    }

    public void setTrustedIssuers(Set<X500Principal> issuers) {
        this.trustedIssuers = Set.copyOf(issuers);
    }

    public SslContextBuilder copy() {
        SslContextBuilder newBuilder;
        try {
            newBuilder = new SslContextBuilder();
            newBuilder.sslContextName = sslContextName;
            newBuilder.sslProviderName = sslProviderName;
            newBuilder.sslProviderClass = sslProviderClass;
            newBuilder.keyManagerAlgorithm = keyManagerAlgorithm;
            newBuilder.trustManagerAlgorithm = trustManagerAlgorithm;
            newBuilder.secureRandom = secureRandom;
            newBuilder.classLoader = classLoader;
            newBuilder.trusts = trusts;
            newBuilder.trustedIssuers = trustedIssuers != null ? Set.copyOf(trustedIssuers) : null;
            newBuilder.clientAlias = clientAlias;
            newBuilder.secureProvider = getSecureProvider();
            newBuilder.keyManagerFactory = getKeyManagerFactory();
            newBuilder.trustManagerFactory = getTrustManagerFactory();
        } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new IllegalStateException("Unusable builder", e);
        }

        return newBuilder;
    }

    private X500Principal resolvePrincipal(String source) {
        try {
            return new X500Principal(source);
        } catch (IllegalArgumentException e) {
            logger.error("'{}' can't be parsed as a DN", source);
            return null;
        }
    }

    private KeyStore getKeyStore(Object trusts) {
        try {
            if (trusts instanceof KeyStore) {
                return (KeyStore) trusts;
            } else {
                MultiKeyStoreProvider.SubKeyStore param = new MultiKeyStoreProvider.SubKeyStore();
                if (trusts instanceof Object[]) {
                    Arrays.stream((Object[]) trusts).forEach(i -> param.addSubStore(i.toString()));
                } else {
                    param.addSubStore(trusts.toString());
                }
                KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
                ks.load(param);
                return ks;
            }
        } catch (KeyStoreException | NoSuchProviderException | IOException | NoSuchAlgorithmException |
                 CertificateException ex) {
            throw new IllegalArgumentException("Failed to load key store", ex);
        }
    }

    private Provider loadByName(ClassLoader cl, String providerClassName) throws NoSuchProviderException {
        try {
            cl = Optional.ofNullable(cl).orElse(SslContextBuilder.class.getClassLoader());
            @SuppressWarnings("unchecked")
            Class<Provider> clazz = (Class<Provider>) cl.loadClass(providerClassName);
            return clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            NoSuchProviderException nspe = new NoSuchProviderException("Can't load custom security provider: " + Helpers.resolveThrowableException(e));
            nspe.addSuppressed(e);
            throw nspe;
        }
    }

    private <T> T doProvide(String name, Provider provider, GetInstanceWithProvider<T> p1, GetInstance<T> p2) throws NoSuchAlgorithmException {
        if (provider != null) {
            try {
                return p1.getInstance(name, provider);
            } catch (NoSuchAlgorithmException ex) {
                // Just fails, will try without an explicit provider
            }
        }
        return p2.getInstance(name);
    }

    public void setSecureRandom(String secureRandom) {
        try {
            this.secureRandom = SecureRandom.getInstance(secureRandom);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unavailable secure random \"" + secureRandom + '"', e);
        }
    }
}
