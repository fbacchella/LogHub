package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestX509Parser {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testDecode() throws IOException, ProcessorException {
        X509Parser.Builder builder = X509Parser.getBuilder();
        builder.setInPlace(true);
        builder.setField(VariablePath.of("x509", "certificate"));
        builder.setDestination(VariablePath.of("x509"));
        X509Parser parser = builder.build();
        for (String pemSource: List.of("/crypto/package1.pem", "/crypto/package2.pem", "/crypto/package3.pem",
                                       "/crypto/package4.pem","/crypto/sampleca.pem", "/crypto/isrgrootx1.pem", "/crypto/isrg-root-x2.pem")) {
            String pemContent;
            try (InputStream is = TestX509Parser.class.getResourceAsStream(pemSource)) {
                pemContent = new String(is.readAllBytes(), StandardCharsets.US_ASCII);
            }
            Event e = factory.newEvent();
            e.putAtPath(VariablePath.of("x509", "certificate"), pemContent);
            e.process(parser);
            if (e.getAtPath(VariablePath.of("x509", "serial_number")) instanceof String) {
                checkCertificate((Map<String, Object>) e.getAtPath(VariablePath.of("x509")));
            } else if (e.getAtPath(VariablePath.of("x509")) instanceof List) {
                for (Map<String, Object> cert: (List<Map<String, Object>>)e.getAtPath(VariablePath.of("x509"))) {
                    checkCertificate(cert);
                }
            }
        }
    }

    @Test
    public void readp12()
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, ProcessorException,
                           UnrecoverableEntryException {
        X509Parser.Builder builder = X509Parser.getBuilder();
        builder.setInPlace(true);
        builder.setField(VariablePath.of("x509", "certificate"));
        builder.setDestination(VariablePath.of("x509"));
        X509Parser parser = builder.build();

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = TestX509Parser.class.getResourceAsStream("/loghub.p12")) {
            keyStore.load(is, "".toCharArray());
        }
        KeyStore.PasswordProtection emptyPassword = new KeyStore.PasswordProtection("".toCharArray());
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            KeyStore.Entry entry = keyStore.getEntry(alias, emptyPassword);
            if (entry instanceof KeyStore.PrivateKeyEntry) {
                Certificate[] chain = keyStore.getCertificateChain(alias);
                for (Certificate cert: chain) {
                    Event e = factory.newEvent();
                    e.putAtPath(VariablePath.of("x509", "certificate"), cert);
                    Assert.assertTrue(e.process(parser));
                    checkCertificate((Map<String, Object>) e.getAtPath(VariablePath.of("x509")));
                }
            } else if (entry instanceof KeyStore.TrustedCertificateEntry) {
                Certificate cert = keyStore.getCertificate(alias);
                Event e = factory.newEvent();
                e.putAtPath(VariablePath.of("x509", "certificate"), cert);
                Assert.assertTrue(e.process(parser));
                checkCertificate((Map<String, Object>) e.getAtPath(VariablePath.of("x509")));
            }
        }
    }

    @Test
    public void testFailed() throws ProcessorException {
        X509Parser.Builder builder = X509Parser.getBuilder();
        builder.setInPlace(true);
        builder.setField(VariablePath.of("x509", "certificate"));
        builder.setDestination(VariablePath.of("x509"));
        X509Parser parser = builder.build();
        Event e = factory.newEvent();
        e.putAtPath(VariablePath.of("x509", "certificate"), "");
        Assert.assertTrue(e.process(parser));
        Assert.assertThrows(ProcessorException.class, () -> {
            Event e2 = factory.newEvent();
            e2.putAtPath(VariablePath.of("x509", "certificate"), new byte[]{1});
            e2.process(parser);
        });
        Assert.assertThrows(ProcessorException.class, () -> {
            Event e2 = factory.newEvent();
            e2.putAtPath(VariablePath.of("x509", "certificate"), "-----BEGIN CERTIFICATE-----\n\n----END CERTIFICATE-----");
            e2.process(parser);
        });
    }

    private void checkCertificate(Map<String, Object> cert) {
        Assert.assertEquals(3, cert.get("version_number"));
        switch (cert.get("serial_number").toString()) {
        case "1":
            Assert.assertTrue((Boolean) cert.get("is_root"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals(2048, cert.get("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("CN=loghub CA", getDn(cert, "subject"));
            Assert.assertEquals("CN=loghub CA", getDn(cert, "issuer"));
            Assert.assertEquals(4696127220000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1540453620000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "41d29dd172eaeea780c12c6ce92f8752":
            Assert.assertTrue((Boolean) cert.get("is_root"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertFalse(cert.containsKey("public_key_exponent"));
            Assert.assertFalse(cert.containsKey("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("SHA384withECDSA", cert.get("signature_algorithm"));
            Assert.assertEquals("EC", cert.get("public_key_algorithm"));
            Assert.assertEquals("secp384r1", cert.get("public_key_curve"));
            Assert.assertEquals("CN=ISRG Root X2,O=Internet Security Research Group,C=US", getDn(cert, "subject"));
            Assert.assertEquals("CN=ISRG Root X2,O=Internet Security Research Group,C=US", getDn(cert, "issuer"));
            Assert.assertEquals(2231510400000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1599177600000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "8210cfb0d240e3594463e0bb63828b00":
            Assert.assertTrue((Boolean) cert.get("is_root"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertEquals(4096, cert.get("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("CN=ISRG Root X1,O=Internet Security Research Group,C=US", getDn(cert, "subject"));
            Assert.assertEquals("CN=ISRG Root X1,O=Internet Security Research Group,C=US", getDn(cert, "issuer"));
            Assert.assertEquals(2064567878000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1433415878000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "59cd1d88":
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(2048, cert.get("public_key_size"));
            Assert.assertEquals(NullOrMissingValue.MISSING, cert.get("is_root"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("CN=localhost", getDn(cert, "subject"));
            Assert.assertEquals("CN=localhost", getDn(cert, "issuer"));
            Assert.assertEquals(5545901064000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1506614664000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "5d938d306736c8061d1ac754846907":
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(4096, cert.get("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("OU=AC RAIZ FNMT-RCM,O=FNMT-RCM,C=ES", getDn(cert, "subject"));
            Assert.assertEquals("OU=AC RAIZ FNMT-RCM,O=FNMT-RCM,C=ES", getDn(cert, "issuer"));
            Assert.assertEquals(1893456000000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1225295996000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "7777062726a9b17c":
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(2048, cert.get("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("CN=AffirmTrust Commercial,O=AffirmTrust,C=US", getDn(cert, "subject"));
            Assert.assertEquals("CN=AffirmTrust Commercial,O=AffirmTrust,C=US", getDn(cert, "issuer"));
            Assert.assertEquals(1924956366000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1264773966000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "6d8c1446b1a60aee":
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertEquals(4096, cert.get("public_key_size"));
            Assert.assertEquals(true, cert.get("is_root"));
            Assert.assertEquals("SHA384withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("CN=AffirmTrust Premium,O=AffirmTrust,C=US", getDn(cert, "subject"));
            Assert.assertEquals("CN=AffirmTrust Premium,O=AffirmTrust,C=US", getDn(cert, "issuer"));
            Assert.assertEquals(2240575836000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1264774236000L, ((Date)cert.get("not_before")).getTime());
            break;
        case "2":
            if ("CN=Buypass Class 2 Root CA,O=Buypass AS-983163327,C=NO".equals(getDn(cert, "subject"))) {
                Assert.assertFalse(cert.containsKey("alternative_names"));
                Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
                Assert.assertEquals(4096, cert.get("public_key_size"));
                Assert.assertEquals(true, cert.get("is_root"));
                Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
                Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
                Assert.assertEquals("CN=Buypass Class 2 Root CA,O=Buypass AS-983163327,C=NO", getDn(cert, "subject"));
                Assert.assertEquals("CN=Buypass Class 2 Root CA,O=Buypass AS-983163327,C=NO", getDn(cert, "issuer"));
                Assert.assertEquals(2234853483000L, ((Date)cert.get("not_after")).getTime());
                Assert.assertEquals(1288082283000L, ((Date)cert.get("not_before")).getTime());
            } else {
                Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
                Assert.assertEquals(2048, cert.get("public_key_size"));
                Assert.assertEquals(NullOrMissingValue.MISSING, cert.get("is_root"));
                Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
                Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
                Assert.assertEquals("CN=localhost", getDn(cert, "subject"));
                Assert.assertEquals("CN=loghub CA", getDn(cert, "issuer"));
                Assert.assertEquals(4696127220000L, ((Date)cert.get("not_after")).getTime());
                Assert.assertEquals(1540454040000L, ((Date)cert.get("not_before")).getTime());
                @SuppressWarnings("unchecked")
                List<String> altNames = (List<String>) cert.get("alternative_names");
                Assert.assertEquals("DNS:localhost", altNames.get(0));
                Assert.assertEquals("IP:127.0.0.1", altNames.get(1));
                // Yes the certificate is wrong
                Assert.assertEquals("DNS:::1", altNames.get(2));
            }
            break;
        case "3":
            Assert.assertFalse(cert.containsKey("alternative_names"));
            Assert.assertEquals(BigInteger.valueOf(65537), cert.get("public_key_exponent"));
            Assert.assertEquals(2048, cert.get("public_key_size"));
            Assert.assertEquals(NullOrMissingValue.MISSING, cert.get("is_root"));
            Assert.assertEquals("SHA256withRSA", cert.get("signature_algorithm"));
            Assert.assertEquals("RSA", cert.get("public_key_algorithm"));
            Assert.assertEquals("CN=junit", getDn(cert, "subject"));
            Assert.assertEquals("CN=loghub CA", getDn(cert, "issuer"));
            Assert.assertEquals(4696127220000L, ((Date)cert.get("not_after")).getTime());
            Assert.assertEquals(1540454160000L, ((Date)cert.get("not_before")).getTime());
            break;
        default:
            Assert.fail(cert.get("serial_number").toString());
        }
    }

    private String getDn(Map<String, Object> cert, String key) {
        Map<String, String> subject = (Map<String, String>) cert.get(key);
        return subject.get("distinguished_name");
    }

    @Test
    public void test_loghub_processors_X509Parser() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.X509Parser"
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
