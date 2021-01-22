package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestJournald {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Journald", "loghub.netty", "loghub.EventsProcessor", "loghub.security", "loghub.netty.http", "loghub.configuration");
    }

    private Journald receiver = null;
    private PriorityBlockingQueue queue;
    private String hostname;
    private int port;

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    public Journald makeReceiver(Consumer<Journald.Builder> prepare, Map<String, Object> propsMap) throws IOException {
        hostname =  InetAddress.getLoopbackAddress().getHostAddress();
        port = Tools.tryGetPort();

        queue = new PriorityBlockingQueue();

        Journald.Builder httpbuilder = Journald.getBuilder();
        httpbuilder.setHost(hostname);
        httpbuilder.setPort(port);
        prepare.accept(httpbuilder);

        receiver = httpbuilder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testhttp", null));

        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
        return receiver;
    }
    
    @Test
    public void testStart() throws IOException {
        makeReceiver( i -> {}, Collections.emptyMap());
    }

    @Test
    public void test_loghub_receivers_Journald() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Journald"
                              ,BeanInfo.build("useJwt", Boolean.TYPE)
                              ,BeanInfo.build("user", String.class)
                              ,BeanInfo.build("password", String.class)
                              ,BeanInfo.build("jaasName", String.class)
                              ,BeanInfo.build("withSSL", Boolean.TYPE)
                              ,BeanInfo.build("SSLClientAuthentication", String.class)
                              ,BeanInfo.build("SSLKeyAlias", String.class)
                        );
    }

}
