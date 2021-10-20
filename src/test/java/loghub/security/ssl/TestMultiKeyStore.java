package loghub.security.ssl;

import java.io.IOException;
import java.net.URI;
import java.security.DomainLoadStoreParameter;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.security.ssl.MultiKeyStoreProvider.SubKeyStore;

public class TestMultiKeyStore {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security.ssl");
    }

    @Test
    public void loadmulti() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("src/test/resources/loghub.p12", "src/test/resources/crypto/package1.pem");

        // Extracted from package1.pem
        Assert.assertTrue(mks.engineEntryInstanceOf("cn=localhost", KeyStore.PrivateKeyEntry.class));
        Assert.assertTrue(mks.engineIsKeyEntry("cn=localhost"));
        // Extracted from loghub.p12
        Assert.assertTrue(mks.engineEntryInstanceOf("loghub ca", KeyStore.PrivateKeyEntry.class));
        Assert.assertTrue(mks.engineEntryInstanceOf("localhost (loghub ca)", KeyStore.PrivateKeyEntry.class));
        Assert.assertTrue(mks.engineEntryInstanceOf("junit (loghub ca)", KeyStore.PrivateKeyEntry.class));
        Assert.assertTrue(mks.engineEntryInstanceOf("password", KeyStore.SecretKeyEntry.class));
        Assert.assertEquals(5, Collections.list(mks.engineAliases()).size());
    }
    
    @Test
    public void loadDomain() throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        DomainLoadStoreParameter params = new DomainLoadStoreParameter(URI.create("file:src/test/resources/crypto/domain.policy"), Collections.emptyMap());
        KeyStore ks = KeyStore.getInstance("DKS");
        ks.load(params);

        MultiKeyStoreSpi mks = load("src/test/resources/crypto/domain.policy");

        // Domain keystore is buggy, just check consistency of results
        Assert.assertEquals(ks.entryInstanceOf("loghubca loghub ca",KeyStore.PrivateKeyEntry.class),
                            mks.engineEntryInstanceOf("loghubca loghub ca", KeyStore.PrivateKeyEntry.class));
        Assert.assertEquals(ks.entryInstanceOf("loghubca localhost (loghub ca)", KeyStore.PrivateKeyEntry.class),
                            mks.engineEntryInstanceOf("loghubca localhost (loghub ca)", KeyStore.PrivateKeyEntry.class));
        Assert.assertEquals(ks.entryInstanceOf("loghubca junit (loghub ca)", KeyStore.PrivateKeyEntry.class),
                            mks.engineEntryInstanceOf("loghubca junit (loghub ca)", KeyStore.PrivateKeyEntry.class));
        Assert.assertEquals(ks.entryInstanceOf("loghubca password", KeyStore.SecretKeyEntry.class),
                            mks.engineEntryInstanceOf("loghubca password", KeyStore.SecretKeyEntry.class));
        Assert.assertEquals(4, Collections.list(mks.engineAliases()).size());
   }

    @Test
    public void loadinversed() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/package2.pem");

        Assert.assertTrue(mks.engineEntryInstanceOf("cn=localhost", KeyStore.PrivateKeyEntry.class));
    }

    @Test
    public void loadduplicate() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/package1.pem", "src/test/resources/crypto/package2.pem");

        Assert.assertTrue(mks.engineEntryInstanceOf("cn=localhost", KeyStore.PrivateKeyEntry.class));
    }

    @Test
    public void loadcacert() throws NoSuchAlgorithmException, CertificateException, IOException {
        String javahome = System.getProperty("java.home");
        MultiKeyStoreSpi mks = load(javahome + "/lib/security/cacerts");

        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadsampleca() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/sampleca.pem");

        Assert.assertEquals(4, Collections.list(mks.engineAliases()).size());
    }

    @Test
    public void loaddefault() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("default");
        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadsystem() throws NoSuchAlgorithmException, CertificateException, IOException {
        MultiKeyStoreSpi mks = load("system");
        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadfailedpem() throws NoSuchAlgorithmException, CertificateException, IOException {
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, 
                                                          () -> load("src/test/resources/crypto/junitkey.pem"));
        Assert.assertEquals("Unknown PEM entry in file src/test/resources/crypto/junitkey.pem", ex.getMessage());
    }

    private MultiKeyStoreSpi load(String... paths) throws NoSuchAlgorithmException, CertificateException, IOException {
        SubKeyStore sks = new SubKeyStore();
        for (String p: paths) {
            sks.addSubStore(p, "");
        }
        MultiKeyStoreSpi mks = new MultiKeyStoreSpi();
        mks.engineLoad(sks);
        Assert.assertEquals(mks.engineSize(), Collections.list(mks.engineAliases()).size());
        return mks;
    }

}
