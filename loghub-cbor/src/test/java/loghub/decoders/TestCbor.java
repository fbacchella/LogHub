package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestCbor {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.decoders.Cbor");
    }

    @Test
    public void testReadConfig() throws IOException {
        String configFile = "input {loghub.receivers.Udp{decoder: loghub.decoders.Cbor }}  | $main pipeline[main] {}";
        Properties p =  Configuration.parse(new StringReader(configFile));
        p.receivers.stream().findAny().get();
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Cbor"
                , BeanChecks.BeanInfo.build("eventsFactory", EventsFactory.class)
        );
    }

}
