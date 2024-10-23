package loghub.security.ssl;

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

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security.ssl");
    }

    @Test
    public void testContextLoader() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "TLSV1.2");
        properties.put("providerclass", "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider");
        properties.put("ephemeralDHKeySize", 1024);
        properties.put("rejectClientInitiatedRenegotiation", 1024);
        properties.put("trusts", Tools.getDefaultKeyStore());
        properties.put("issuers", new Object[] {"CN=Issuer,DC=loghub,DC=fr"});

        SSLContext newCtxt = SslContextBuilder.getBuilder(properties).build();
        Assert.assertNotNull(newCtxt);
        Assert.assertEquals("TLSV1.2", newCtxt.getProtocol());
        Assert.assertEquals("BCJSSE", newCtxt.getProvider().getName());
    }

    @Test
    public void testContextLoaderFailed() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("context", "NOTLS");

        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> SslContextBuilder.getBuilder(TestSSL.class.getClassLoader(), properties).build()
        );
        Assert.assertEquals("NOTLS SSLContext not available", ex.getCause().getMessage());
    }

}
