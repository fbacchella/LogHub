package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestFork {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test(timeout = 5000)
    public void testFork() throws ConfigException, IOException, InterruptedException {
        String confile = "pipeline[newpipe] {} pipeline[main] { +$newpipe}";
        Properties conf = Tools.loadConf(new StringReader(confile));

        Map<String, Object> m =  new HashMap<>();
        InetAddress inet4Address = InetAddress.getByName("8.8.8.8");
        InetAddress inet6Address = InetAddress.getByName("2001:4860:4860::8888");
        Map<?, ?> boolMap = new HashMap<>(Map.of("a", true, "b", false));
        Map<?, ?> intMap = new HashMap<>(Map.of("a", (byte) 1, "b", (short) 2, "c", 3, "d", 4L));
        Map<?, ?> floatMap = new HashMap<>(Map.of("a", 1.0f, "b", 2.0));
        Map<?, ?> textMap = new HashMap<>(Map.of("a", 'a', "b", "b"));
        Map<?, ?> timeMap = new HashMap<>(Map.of("a", new Date(1), "b", Instant.ofEpochSecond(2, 3)));
        InetSocketAddress inetSocketAddress = new InetSocketAddress("8.8.8.8", 53);
        List<Object> list = new ArrayList<>(List.of(ConnectionContext.EMPTY));
        list.add(null);
        Instant now = Instant.now();
        ZonedDateTime zdtNow = ZonedDateTime.now();
        Map<DayOfWeek, Integer> daysMapping = Arrays.stream(DayOfWeek.values()).collect(Collectors.toMap(
                d -> d, DayOfWeek::getValue,
                (a, b) -> b,
                () -> new EnumMap<>(DayOfWeek.class)));
        List<Event> received = new ArrayList<>();
        Event processed = Tools.processEventWithPipeline(factory, conf, "main", e -> {
            m.put("boolMap", boolMap);
            m.put("intMap", intMap);
            m.put("floatMap", floatMap);
            m.put("textMap", textMap);
            m.put("timeMap", timeMap);
            m.put("list", list);
            m.put("inet4Address", inet4Address);
            m.put("inet6Address", inet6Address);
            m.put("inetSocketAddress", inetSocketAddress);
            e.put("message", m);
            e.put("arrayBool", new Boolean[]{true, false});
            e.put("arrayInt", new Integer[]{1, 2});
            e.put("arrayInstant", new Temporal[]{now, zdtNow});
            e.put("uuid", new Instant[]{Instant.ofEpochSecond(0)});
            e.put("null", new Object[]{null, NullOrMissingValue.NULL});
            e.put("emptyMap", Map.of());
            e.put("staticMap", Map.of(1, 2));
            e.put("daysMapping", daysMapping);
            e.putMeta("meta", 1);
        }, () -> {
            try {
                Event r = conf.mainQueue.poll(2, TimeUnit.SECONDS);
                Assert.assertNotNull(r);
                received.add(r);
                Assert.assertNull(conf.mainQueue.poll(1, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        received.add(processed);
        Consumer<Event> checkEvent = e -> {
            Map<?, ?> message = (Map<?, ?>) e.get("message");
            Assert.assertEquals(boolMap, message.get("boolMap"));
            Assert.assertEquals(intMap, message.get("intMap"));
            Assert.assertEquals(floatMap, message.get("floatMap"));
            Assert.assertEquals(textMap, message.get("textMap"));
            Assert.assertEquals(timeMap, message.get("timeMap"));
            @SuppressWarnings("unchecked")
            List<Object> newList = (List<Object>)message.get("list");
            Assert.assertSame(ConnectionContext.EMPTY, newList.get(0));
            Assert.assertEquals(NullOrMissingValue.NULL, newList.get(1));
            Assert.assertEquals(1, e.getMeta("meta"));
            Assert.assertSame(inet4Address, message.get("inet4Address"));
            Assert.assertSame(inet6Address, message.get("inet6Address"));
            Assert.assertSame(inetSocketAddress, message.get("inetSocketAddress"));
            Assert.assertArrayEquals(new Boolean[]{true, false}, (Boolean[]) e.get("arrayBool"));
            Assert.assertArrayEquals(new Integer[]{1, 2}, (Integer[]) e.get("arrayInt"));
            Temporal[] arrayInstant = (Temporal[]) e.get("arrayInstant");
            Assert.assertEquals(2, arrayInstant.length);
            Assert.assertSame(now, arrayInstant[0]);
            Assert.assertSame(zdtNow, arrayInstant[1]);
            Assert.assertSame(Map.of(), e.get("emptyMap"));
            Assert.assertEquals(Map.of(1, 2), e.get("staticMap"));
            Assert.assertEquals(daysMapping, e.get("daysMapping"));
        };
        Assert.assertEquals(2, received.size());
        for (Event ev: received) {
            checkEvent.accept(ev);
        }
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Forker"
                              , BeanChecks.BeanInfo.build("destination", String.class)
                        );
    }

}
