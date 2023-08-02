package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
        Map<?, ?> intMap = new HashMap<>(Map.of("a", (byte)1, "b", (short)2, "c", 3, "d", 4L));
        Map<?, ?> floatMap = new HashMap<>(Map.of("a", 1.0f, "b", 2.0));
        Map<?, ?> textMap = new HashMap<>(Map.of("a", 'a', "b", "b"));
        Map<?, ?> timeMap = new HashMap<>(Map.of("a", new Date(1), "b", Instant.ofEpochSecond(2, 3)));
        List<Object> list = new ArrayList<>(List.of(ConnectionContext.EMPTY));
        list.add(null);

        Map<String, Object> m =  new HashMap<>();
        m.put("boolMap", boolMap);
        m.put("intMap", intMap);
        m.put("floatMap", floatMap);
        m.put("textMap", textMap);
        m.put("timeMap", timeMap);
        m.put("list", list);
        InetAddress inetAddress = InetAddress.getByName("8.8.8.8");
        m.put("inetAddress", inetAddress);
        InetAddress inet6Address = InetAddress.getByName("2001:4860:4860::8888");
        m.put("inet6Address", inet6Address);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("8.8.8.8", 53);
        m.put("inetSocketAddress", inetSocketAddress);
        event.put("message", m);
        event.put("arrayBool", new Boolean[]{true, false});
        event.put("arrayInt", new Integer[]{1, 2});
        event.put("arrayInstant", new Instant[]{Instant.ofEpochSecond(0)});
        event.put("uuid", new Instant[]{Instant.ofEpochSecond(0)});

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
        Assert.assertEquals(System.identityHashCode(inetAddress), System.identityHashCode(message.get("inetAddress")));
        Assert.assertEquals(System.identityHashCode(inetSocketAddress), System.identityHashCode(message.get("inetSocketAddress")));
        Assert.assertEquals(System.identityHashCode(inet6Address), System.identityHashCode(message.get("inet6Address")));
        Assert.assertArrayEquals(new Boolean[]{true, false}, (Boolean[])event.get("arrayBool"));
        Assert.assertArrayEquals(new Integer[]{1, 2}, (Integer[])event.get("arrayInt"));
        Assert.assertArrayEquals(new Instant[]{Instant.ofEpochSecond(0)}, (Instant[])event.get("arrayInstant"));
    }

    @Test
    public void test_loghub_processors_Forker() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Forker"
                              , BeanChecks.BeanInfo.build("destination", String.class)
                        );
    }

}
