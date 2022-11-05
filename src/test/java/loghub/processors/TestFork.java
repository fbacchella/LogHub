package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
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
        Event event = factory.newTestEvent();
        Pipeline ppl = new Pipeline(Collections.emptyList(), "main", null);
        event.inject(ppl, conf.mainQueue, true);
        Map<?, ?> boolMap = new HashMap<>(Map.of("a", true, "b", false));
        Map<?, ?> intMap = new HashMap<>(Map.of("a", (byte)1, "b", 2, "c", 3L));
        Map<?, ?> floatMap = new HashMap<>(Map.of("a", 1.0f, "b", 2.0));
        Map<?, ?> textMap = new HashMap<>(Map.of("a", 'a', "b", "b"));
        Map<?, ?> timeMap = new HashMap<>(Map.of("a", new Date(1), "b", Instant.ofEpochSecond(2, 3)));
        List<Object> list = new ArrayList<>(List.of(ConnectionContext.EMPTY));
        list.add(null);

        event.put("message", new HashMap<>(Map.of("boolMap", boolMap, "intMap", intMap, "floatMap", floatMap, "textMap", textMap, "timeMap", timeMap, "list", list)));
        event.putMeta("meta", 1);
        forker.fork(event);

        // Removing the original event
        conf.mainQueue.remove();
        Event forked = conf.mainQueue.remove();
        Map<?, ?> message = (Map<?, ?>) forked.get("message");
        Assert.assertEquals(boolMap, message.get("boolMap"));
        Assert.assertEquals(intMap, message.get("intMap"));
        Assert.assertEquals(floatMap, message.get("floatMap"));
        Assert.assertEquals(textMap, message.get("textMap"));
        Assert.assertEquals(timeMap, message.get("timeMap"));
        Assert.assertEquals(list, message.get("list"));
        Assert.assertEquals(1, forked.getMeta("meta"));
        Assert.assertEquals(ConnectionContext.EMPTY, forked.getConnectionContext());
    }

    @Test
    public void test_loghub_processors_Forker() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Forker"
                              , BeanChecks.BeanInfo.build("destination", String.class)
                        );
    }

}
