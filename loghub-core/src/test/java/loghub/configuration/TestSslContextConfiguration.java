package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.receivers.TcpLinesStream;

public class TestSslContextConfiguration {

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration");
    }

    @Test
    public void loadConf() throws IOException {
        String config = "input { loghub.receivers.TcpLinesStream { port: 0, decoder: loghub.decoders.StringCodec, sslContext: {trusts: [\"target/test-classes/loghub.p12\"]} }} | $main pipeline[main]{}";
        StringReader configReader = new StringReader(config);
        Properties p = Tools.loadConf(configReader);
        TcpLinesStream receiver = (TcpLinesStream) p.receivers.stream().findAny().get();
        Assert.assertNotEquals(p.ssl, receiver.getSslContext());
    }

}
