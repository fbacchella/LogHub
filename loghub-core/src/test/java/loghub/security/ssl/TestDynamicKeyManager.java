package loghub.security.ssl;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.X509ExtendedKeyManager;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestDynamicKeyManager {

    private PrivateKeyEntry generateCertificate(String dn, List<String> altNames, boolean asServer) throws Exception {
        KeyPair keyPair = generateKeyPair();

        long now = System.currentTimeMillis();
        Instant startDate = Instant.now();
        Instant endDate = startDate.plus(365, ChronoUnit.DAYS);

        BigInteger serial = BigInteger.valueOf(now);
        X500Name subject = new X500Name(dn);

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                subject,
                serial,
                Date.from(startDate),
                Date.from(endDate),
                subject,
                keyPair.getPublic()
        );
        if (asServer){
            certBuilder.addExtension(
                    org.bouncycastle.asn1.x509.Extension.extendedKeyUsage,
                    false, // l’extension n’est pas critique
                    new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth)
            );
        } else {
            certBuilder.addExtension(
                    org.bouncycastle.asn1.x509.Extension.extendedKeyUsage,
                    false, // l’extension n’est pas critique
                    new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth)
            );
        }

        // Ajout des Subject Alternative Names
        if (altNames != null && !altNames.isEmpty()) {
            GeneralName[] generalNames = new GeneralName[altNames.size()];
            for (int i = 0; i < altNames.size(); i++) {
                generalNames[i] = new GeneralName(GeneralName.dNSName, altNames.get(i));
            }
            GeneralNames subjectAltNames = new GeneralNames(generalNames);
            certBuilder.addExtension(
                    org.bouncycastle.asn1.x509.Extension.subjectAlternativeName,
                    false,
                    subjectAltNames
            );
        }

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                                       .build(keyPair.getPrivate());

        X509Certificate certificate = new JcaX509CertificateConverter()
                                              .getCertificate(certBuilder.build(signer));

        certificate.verify(keyPair.getPublic());

        return new PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate});
    }

    private KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());
        return keyGen.generateKeyPair();
    }

    public static X509ExtendedKeyManager buildTrustManagersFromCertificates(Collection<PrivateKeyEntry> certs)
            throws Exception {

        if (certs == null || certs.isEmpty()) {
            throw new IllegalArgumentException("No certificates supplied.");
        }

        // In-memory KeyStore using platform default type (typically JKS or PKCS12)
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);

        int idx = 0;
        for (PrivateKeyEntry c : certs) {
            String alias = "cert-" + (idx++);
            ks.setEntry(alias, c, new PasswordProtection(new char[]{}));
        }

        KeyManagerFactory tmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        tmf.init(ks, "".toCharArray());

        return (X509ExtendedKeyManager)tmf.getKeyManagers()[0];
    }

    @Test
    public void testGeneric() throws Exception {
        String dn = "CN=loghub.com";
        PrivateKeyEntry certOther = generateCertificate(dn, List.of("www.google.com"), false);
        PrivateKeyEntry certClient = generateCertificate(dn, List.of("loghub.com", "www.loghub.com"), false);
        PrivateKeyEntry certServer = generateCertificate(dn, List.of("loghub.com", "www.loghub.com"), true);
        DynamicKeyManager kmtranslator = new DynamicKeyManager(buildTrustManagersFromCertificates(List.of(certOther, certClient, certServer)), null, null);
        Assert.assertEquals("cert-2", kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("loghub.com"))));
        Assert.assertEquals("cert-2", kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("www.loghub.com"))));
        // Second run, should use the cache
        Assert.assertEquals("cert-2", kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("loghub.com"))));
        Assert.assertEquals("cert-2", kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("www.loghub.com"))));
        Assert.assertEquals("cert-2", kmtranslator.sniCache.get("RSA").get(new SNIHostName("loghub.com")));
        Assert.assertEquals("cert-2", kmtranslator.sniCache.get("RSA").get(new SNIHostName("www.loghub.com")));
    }

    @Test
    public void testWildcardCertificate() throws Exception {
        String dn = "CN=loghub.com";
        List<String> altNames = new ArrayList<>();
        altNames.add("*.loghub.com");

        PrivateKeyEntry cert = generateCertificate(dn, altNames, true);
        DynamicKeyManager kmtranslator = new DynamicKeyManager(buildTrustManagersFromCertificates(List.of(cert)), null, null);
        String alias = kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("www.loghub.com")));
        Assert.assertEquals("cert-0", alias);
    }

    @Test
    public void testPunyCode() throws Exception {
        String dn = "CN=loghub.com";
        List<String> altNames = new ArrayList<>();
        altNames.add("xn--uf-via.com");

        PrivateKeyEntry cert = generateCertificate(dn, altNames, true);
        DynamicKeyManager kmtranslator = new DynamicKeyManager(buildTrustManagersFromCertificates(List.of(cert)), null, null);
        String alias = kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("xn--uf-via.com")));
        Assert.assertEquals("cert-0", alias);
    }

    @Test
    public void testClientCert() throws Exception {
        String dn = "CN=loghub.com";
        List<String> altNames = new ArrayList<>();
        altNames.add("xn--uf-via.com");

        PrivateKeyEntry cert = generateCertificate(dn, altNames, false);
        DynamicKeyManager kmtranslator = new DynamicKeyManager(buildTrustManagersFromCertificates(List.of(cert)), null, null);
        String alias = kmtranslator.resolveWithSni("RSA", null, List.of(new SNIHostName("xn--uf-via.com")));
        Assert.assertNull(alias);
    }

}
