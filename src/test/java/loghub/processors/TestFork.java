package loghub.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.Pipeline;
import loghub.events.Event;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestFork {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testFork() throws ConfigException, IOException {
        String confile = "pipeline[newpipe] {}";

        Properties conf = Tools.loadConf(new StringReader(confile));
        Forker forker = new Forker();
        forker.setDestination("newpipe");
        Assert.assertTrue(forker.configure(conf));
        ConnectionContext<?> ipctx = new IpConnectionContext(new InetSocketAddress("localhost", 0), new InetSocketAddress("localhost", 0), null);
        Event event = factory.newTestEvent(ipctx);
        Pipeline ppl = new Pipeline(Collections.emptyList(), "main", null);
        event.inject(ppl, conf.mainQueue, true);
        event.put("message", "tofork");
        event.putMeta("meta", 1);
        forker.fork(event);

        // Removing the original event
        conf.mainQueue.remove();
        Event forked = conf.mainQueue.remove();
        assertEquals("tofork", forked.get("message"));
        assertEquals(1, forked.getMeta("meta"));
        assertNotEquals(ipctx, forked.getConnectionContext());
        assertTrue(IpConnectionContext.class.equals(forked.getConnectionContext().getClass()));
    }

    @Test
    public void test_loghub_processors_Forker() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Forker"
                              , BeanChecks.BeanInfo.build("destination", String.class)
                        );
    }

}
