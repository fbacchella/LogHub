package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestNsca {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.Nsca");
    }

    @Test
    public void test() throws ConfigException, IOException {
        String conf = "pipeline[main] {} output $main | { loghub.senders.Nsca { password: \"password\", encryption: \"RIJNDAEL192\", nagiosServer: \"localhost\", largeMessageSupport: true, mapping: { \"level\": [level], \"service\": [service],  \"message\": \"message\",  \"host\": [host], } } }";

        Properties p = Configuration.parse(new StringReader(conf));

        Nsca sender = (Nsca) p.senders.stream().findAny().get();
        Assert.assertTrue(sender.configure(p));
        Event ev = factory.newEvent();
        ev.put("level", "CRITICAL");
        ev.put("service", "aservice");
        ev.put("message", "message");
        ev.put("host", "host");

        Assert.assertThrows("Connection refused", SendException.class, () -> sender.send(ev));
        sender.stopSending();
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.Nsca"
                , BeanChecks.BeanInfo.build("port", Integer.TYPE)
                , BeanChecks.BeanInfo.build("nagiosServer", String.class)
                , BeanChecks.BeanInfo.build("password", String.class)
                , BeanChecks.BeanInfo.build("connectTimeout", Integer.TYPE)
                , BeanChecks.BeanInfo.build("timeout", Integer.TYPE)
                , BeanChecks.BeanInfo.build("largeMessageSupport", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("encryption", String.class)
                , BeanChecks.BeanInfo.build("mapping", Map.class)
        );
    }

}
