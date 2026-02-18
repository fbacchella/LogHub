package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
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

    private void run(Supplier<Event> eventSource, Consumer<Event> tests) throws ProcessorException {
        Gatherer.Builder builder = new Gatherer.Builder();
        builder.setField(VariablePath.of("message"));
        Gatherer gatherer = builder.build();
        Event ev = eventSource.get();
        Assertions.assertTrue(gatherer.process(ev));
        tests.accept(ev);
    }

    @Test
    void testGather() throws ProcessorException {
        run(
                () -> {
                    Event ev = factory.newEvent();
                    ev.putAtPath(VariablePath.of("message", "a"), List.of(1, 2));
                    ev.putAtPath(VariablePath.of("message", "b"), List.of(3, 4));
                    return ev;
                },
                ev -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> result = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("message"));
                    Assertions.assertEquals(2, result.size());

                    Map<?, ?> m1 = result.getFirst();
                    Assertions.assertEquals(1, m1.get("a"));
                    Assertions.assertEquals(3, m1.get("b"));

                    Map<?, ?> m2 = result.get(1);
                    Assertions.assertEquals(2, m2.get("a"));
                    Assertions.assertEquals(4, m2.get("b"));
                }
        );
    }

    @Test
    void testGatherWithDifferentSize() throws ProcessorException {
        run(
                () -> {
                    Event ev = factory.newEvent();
                    ev.putAtPath(VariablePath.of("message", "a"), List.of(1, 2));
                    ev.putAtPath(VariablePath.of("message", "b"), List.of(3));
                    return ev;
                },
                ev -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> result = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("message"));
                    Assertions.assertEquals(2, result.size());

                    Map<?, ?> m1 = result.getFirst();
                    Assertions.assertEquals(1, m1.get("a"));
                    Assertions.assertEquals(3, m1.get("b"));

                    Map<?, ?> m2 = result.get(1);
                    Assertions.assertEquals(2, m2.get("a"));
                    Assertions.assertEquals(1, m2.size());
                }
        );
    }

    @Test
    void testGatherNonIterables() throws ProcessorException {
        run(
                () -> {
                    Event ev = factory.newEvent();
                    ev.putAtPath(VariablePath.of("message", "a"), 1);
                    ev.putAtPath(VariablePath.of("message", "b"), List.of(2, 3));
                    return ev;
                },
                ev -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> result = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("message"));
                    Assertions.assertEquals(2, result.size());

                    Map<?, ?> m1 = result.getFirst();
                    Assertions.assertEquals(1, m1.get("a"));
                    Assertions.assertEquals(2, m1.get("b"));

                    Map<?, ?> m2 = result.get(1);
                    Assertions.assertEquals(NullOrMissingValue.NULL, m2.get("a"));
                    Assertions.assertEquals(3, m2.get("b"));
                }
        );

    }

    @Test
    void testGatherArray() throws ProcessorException {
        run(
                () -> {
                    Event ev = factory.newEvent();
                    ev.putAtPath(VariablePath.of("message", "a"), new String[]{"1", "2"});
                    ev.putAtPath(VariablePath.of("message", "b"), "3");
                    return ev;
                },
                ev -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> result = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("message"));
                    Assertions.assertEquals(2, result.size());

                    Map<?, ?> m1 = result.getFirst();
                    Assertions.assertEquals("1", m1.get("a"));
                    Assertions.assertEquals("3", m1.get("b"));

                    Map<?, ?> m2 = result.get(1);
                    Assertions.assertEquals("2", m2.get("a"));
                    Assertions.assertEquals(NullOrMissingValue.NULL, m2.get("b"));
                }
        );
    }

    @Test
    void testGatherNull() throws ProcessorException {
        run(
                () -> {
                    Event ev = factory.newEvent();
                    ev.putAtPath(VariablePath.of("message", "a"), null);
                    ev.putAtPath(VariablePath.of("message", "b"), List.of(1));
                    return ev;
                },
                ev -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> result = (List<Map<String, Object>>) ev.getAtPath(VariablePath.of("message"));
                    Assertions.assertEquals(1, result.size());

                    Map<?, ?> m1 = result.getFirst();
                    Assertions.assertEquals(NullOrMissingValue.NULL, m1.get("a"));
                    Assertions.assertEquals(1, m1.get("b"));
                }
        );
    }

    @Test
    void testLevelConfusion() throws IOException {
        Properties p =  Configuration.parse(new StringReader("""
        pipeline[main]{
            loghub.processors.Gatherer {
                field: [message]
            }
        }
        """));
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("message"), List.of(1, 2, 3));
        ev.putAtPath(VariablePath.of("b"), 1);
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assertions.assertEquals(1, ev.getAtPath(VariablePath.of("b")));
        Assertions.assertEquals(List.of(1, 2, 3), ev.getAtPath(VariablePath.of("message")));
    }

}
