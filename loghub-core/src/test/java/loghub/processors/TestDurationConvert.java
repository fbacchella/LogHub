package loghub.processors;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.DurationUnit;
import loghub.IgnoredEventException;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static loghub.DurationUnit.CENTI;
import static loghub.DurationUnit.DECI;
import static loghub.DurationUnit.DURATION;
import static loghub.DurationUnit.MICRO;
import static loghub.DurationUnit.MILLI;
import static loghub.DurationUnit.NANO;
import static loghub.DurationUnit.SECOND;
import static loghub.DurationUnit.SECOND_FLOAT;
import static loghub.DurationUnit.STRING;

public class TestDurationConvert {

    private final EventsFactory factory = new EventsFactory();

    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    private Object process(Object value, DurationUnit in, DurationUnit out) throws ProcessorException {
        DurationConvert.Builder builder = DurationConvert.getBuilder();
        builder.setIn(in);
        builder.setOut(out);
        builder.setField(VariablePath.parse("field"));
        DurationConvert parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", value);
        parse.process(event);
        return event.get("field");
    }

    @Test
    public void testConvert() throws ProcessorException {
        Assert.assertEquals(1_000_000L, process("1", MILLI, NANO));
        Assert.assertEquals(1.0, process(1, SECOND_FLOAT, SECOND_FLOAT));
        Assert.assertEquals(1L, process(1, SECOND, SECOND));
        Assert.assertEquals(1L, process(1, DECI, DECI));
        Assert.assertEquals(1L, process(1, CENTI, CENTI));
        Assert.assertEquals(1L, process(1, MILLI, MILLI));
        Assert.assertEquals(1L, process(1, MICRO, MICRO));
        Assert.assertEquals(1L, process(1, NANO, NANO));
        Assert.assertEquals(1.1, process(1.1, SECOND, SECOND_FLOAT));
        Assert.assertEquals(1.1, process("1.1", SECOND, SECOND_FLOAT));
        Assert.assertEquals(1000L, process(1, SECOND, MILLI));
        Assert.assertEquals(1_000L, process(1, MICRO, NANO));
        Assert.assertEquals(10L, process(1, CENTI, MILLI));
        Assert.assertEquals(100L, process(1, DECI, MILLI));
        Assert.assertEquals(1_000_000L, process(1, MILLI, NANO));
        Assert.assertEquals(1_000_000L, process(Duration.ofMillis(1), MILLI, NANO));
        Assert.assertEquals(100L, process(Duration.ofMillis(100), DURATION, MILLI));
        Assert.assertEquals(Duration.ofMillis(1), process(1, MILLI, DURATION));
        Assert.assertEquals(Duration.parse("PT20.345S"), process("PT20.345S", STRING, DURATION));
        Assert.assertEquals("PT20.345S", process(Duration.parse("PT20.345S"), STRING, STRING));
    }

    @Test
    public void testFailure() {
        ProcessorException ex = Assert.assertThrows(ProcessorException.class, () -> process("a", MILLI, NANO));
        Assert.assertEquals("Field with path \"[field]\" invalid: Can't parse duration a", ex.getMessage());
        ProcessorException ex2 = Assert.assertThrows(ProcessorException.class, () -> process(Instant.ofEpochSecond(0), MILLI, NANO));
        Assert.assertEquals("Field with path \"[field]\" invalid: Can't resolve period 1970-01-01T00:00:00Z", ex2.getMessage());
        Assert.assertThrows(IgnoredEventException.class, () -> process(Instant.ofEpochSecond(0), DURATION, NANO));
    }

}
