package loghub.security.ssl;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;

public class TestSSL {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security.ssl");
    }

    @Test
    public void testContextLoader()
            throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException,
            IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSV1.2");
        properties.put("providerclass", "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider");
        properties.put("ephemeralDHKeySize", 1024);
        properties.put("rejectClientInitiatedRenegotiation", 1024);
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.put("issuers", new Object[] {"CN=Issuer,DC=loghub,DC=fr"});

        SSLContext newCtxt = ContextLoader.build(TestSSL.class.getClassLoader(), properties);
        Assert.assertNotNull(newCtxt);
        Assert.assertEquals("TLSV1.2", newCtxt.getProtocol());
        Assert.assertEquals("BCJSSE", newCtxt.getProvider().getName());
    }

    @Test
    public void testContextLoaderFailed() throws NoSuchAlgorithmException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "NOTLS");

        SSLContext newCtxt = ContextLoader.build(TestSSL.class.getClassLoader(), properties);
        Assert.assertNull(newCtxt);
    }

}
