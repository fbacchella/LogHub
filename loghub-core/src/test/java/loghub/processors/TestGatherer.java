package loghub.processors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestGatherer {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");

    }

    @Test
    void testGather() throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        Gatherer gatherer = builder.build();


        Event ev = factory.newEvent();
        Map<String, Object> value = new HashMap<>();
        value.put("a", List.of(1, 2));
        value.put("b", List.of(3, 4));

        Object result = gatherer.fieldFunction(ev, value);

        Assertions.assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        Assertions.assertEquals(2, resultList.size());
        
        Map<?, ?> m1 = (Map<?, ?>) resultList.getFirst();
        Assertions.assertEquals(1, m1.get("a"));
        Assertions.assertEquals(3, m1.get("b"));

        Map<?, ?> m2 = (Map<?, ?>) resultList.get(1);
        Assertions.assertEquals(2, m2.get("a"));
        Assertions.assertEquals(4, m2.get("b"));
    }

    @Test
    void testGatherWithDifferentSize() throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        Gatherer gatherer = builder.build();

        Event ev = factory.newEvent();
        Map<String, Object> value = new HashMap<>();
        value.put("a", List.of(1, 2));
        value.put("b", List.of(3));

        Object result = gatherer.fieldFunction(ev, value);
        Assertions.assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        Assertions.assertEquals(2, resultList.size());
        
        Map<?, ?> m1 = (Map<?, ?>) resultList.getFirst();
        Assertions.assertEquals(1, m1.get("a"));
        Assertions.assertEquals(3, m1.get("b"));

        Map<?, ?> m2 = (Map<?, ?>) resultList.get(1);
        Assertions.assertEquals(2, m2.get("a"));
        Assertions.assertEquals(NullOrMissingValue.NULL, m2.get("b"));
    }

    @Test
    void testGatherNonIterables() throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        Gatherer gatherer = builder.build();

        Event ev = factory.newEvent();
        Map<String, Object> value = new HashMap<>();
        value.put("a", 1);
        value.put("b", List.of(2, 3));

        Object result = gatherer.fieldFunction(ev, value);

        Assertions.assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        Assertions.assertEquals(2, resultList.size());
        
        Map<?, ?> m1 = (Map<?, ?>) resultList.getFirst();
        Assertions.assertEquals(1, m1.get("a"));
        Assertions.assertEquals(2, m1.get("b"));

        Map<?, ?> m2 = (Map<?, ?>) resultList.get(1);
        Assertions.assertEquals(NullOrMissingValue.NULL, m2.get("a"));
        Assertions.assertEquals(3, m2.get("b"));
    }

    @Test
    void testGatherArray() throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        Gatherer gatherer = builder.build();

        Event ev = factory.newEvent();
        Map<String, Object> value = new HashMap<>();
        value.put("a", new String[]{"1", "2"});
        value.put("b", "3");

        Object result = gatherer.fieldFunction(ev, value);

        Assertions.assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        Assertions.assertEquals(2, resultList.size());

        Map<?, ?> m1 = (Map<?, ?>) resultList.getFirst();
        Assertions.assertEquals("1", m1.get("a"));
        Assertions.assertEquals("3", m1.get("b"));

        Map<?, ?> m2 = (Map<?, ?>) resultList.get(1);
        Assertions.assertEquals("2", m2.get("a"));
        Assertions.assertEquals(NullOrMissingValue.NULL, m2.get("b"));
    }

    @Test
    void testGatherNull() throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        Gatherer gatherer = builder.build();

        Event ev = factory.newEvent();
        Map<String, Object> value = new HashMap<>();
        value.put("a", null);
        value.put("b", List.of(1));

        Object result = gatherer.fieldFunction(ev, value);

        Assertions.assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        Assertions.assertEquals(1, resultList.size());

        Map<?, ?> m1 = (Map<?, ?>) resultList.getFirst();
        Assertions.assertEquals(NullOrMissingValue.NULL, m1.get("a"));
        Assertions.assertEquals(1, m1.get("b"));
    }

}
