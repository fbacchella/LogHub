package loghub.security.ssl;

import java.io.IOException;
import java.net.URI;
import java.security.DomainLoadStoreParameter;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.interfaces.RSAPrivateCrtKey;
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

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security.ssl");
    }

    @Test
    public void loadmulti() {
        MultiKeyStoreSpi mks = load("src/test/resources/loghub.p12", "src/test/resources/crypto/package1.pem");

        // Extracted from package1.pem
        Assert.assertTrue(mks.engineEntryInstanceOf("localhost", KeyStore.PrivateKeyEntry.class));
        Assert.assertTrue(mks.engineIsKeyEntry("localhost"));
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
        Assert.assertEquals(ks.entryInstanceOf("loghubca loghub ca", KeyStore.PrivateKeyEntry.class),
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
    public void loadinversed() {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/package2.pem");

        Assert.assertTrue(mks.engineEntryInstanceOf("localhost", KeyStore.PrivateKeyEntry.class));
    }

    @Test
    public void loadwithalias() {
        MultiKeyStoreSpi mks = load("file:src/test/resources/crypto/package1.pem?alias=otheralias");

        Assert.assertTrue(mks.engineEntryInstanceOf("otheralias", KeyStore.PrivateKeyEntry.class));
    }

    @Test
    public void loadduplicate() {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/package1.pem", "src/test/resources/crypto/package2.pem");

        Assert.assertTrue(mks.engineEntryInstanceOf("localhost", KeyStore.PrivateKeyEntry.class));
    }

    @Test
    public void loadwrongpem() {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/openssh.pem");

        Assert.assertEquals(0, mks.engineSize());
    }

    @Test
    public void loadwrongtype() {
        MultiKeyStoreSpi mks = load("src/test/resources/etl.conf");

        Assert.assertEquals(0, mks.engineSize());
    }

    @Test
    public void loadmissing() {
        MultiKeyStoreSpi mks = load("__nofile__.pem");

        Assert.assertEquals(0, mks.engineSize());
    }

    @Test
    public void loadrsakeypem()
            throws NoSuchAlgorithmException, UnrecoverableKeyException {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/package3.pem");
        Assert.assertEquals(1, mks.engineSize());
        String alias = mks.engineAliases().nextElement();
        RSAPrivateCrtKey key = (RSAPrivateCrtKey) mks.engineGetKey(alias, new char[0]);
        Assert.assertEquals(65537, key.getPublicExponent().intValue());
    }

    @Test
    public void loadencryptedpcks8()
            throws NoSuchAlgorithmException, UnrecoverableKeyException {
        MultiKeyStoreSpi mks = load("file:src/test/resources/crypto/package4.pem?password=loghub&alias=encrypted");
        Assert.assertEquals(1, mks.engineSize());
        String alias = mks.engineAliases().nextElement();
        Assert.assertEquals("encrypted", alias);
        RSAPrivateCrtKey key = (RSAPrivateCrtKey) mks.engineGetKey(alias, new char[0]);
        Assert.assertEquals(65537, key.getPublicExponent().intValue());
    }

    @Test
    public void loadcacert() {
        String javahome = System.getProperty("java.home");
        MultiKeyStoreSpi mks = load(javahome + "/lib/security/cacerts");

        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadsampleca() {
        MultiKeyStoreSpi mks = load("src/test/resources/crypto/sampleca.pem");

        Assert.assertEquals(4, Collections.list(mks.engineAliases()).size());
    }

    @Test
    public void loaddefault() {
        MultiKeyStoreSpi mks = load("default");
        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadsystem() {
        MultiKeyStoreSpi mks = load("system");
        Assert.assertTrue(Collections.list(mks.engineAliases()).size() > 1);
    }

    @Test
    public void loadLetsEncrypt() {
        MultiKeyStoreSpi mks = load("https://letsencrypt.org/certs/isrgrootx1.pem.txt?content-type=application/x-pem-file");
        Assert.assertEquals(1, Collections.list(mks.engineAliases()).size());
    }

    private MultiKeyStoreSpi load(String... paths) {
        SubKeyStore sks = new SubKeyStore();
        for (String p : paths) {
            sks.addSubStore(p);
        }
        MultiKeyStoreSpi mks = new MultiKeyStoreSpi();
        mks.engineLoad(sks);
        Assert.assertEquals(mks.engineSize(), Collections.list(mks.engineAliases()).size());
        return mks;
    }

}
