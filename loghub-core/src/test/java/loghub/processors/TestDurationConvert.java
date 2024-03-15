package loghub.processors;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static loghub.processors.DurationConvert.DurationUnit.CENTI;
import static loghub.processors.DurationConvert.DurationUnit.DECI;
import static loghub.processors.DurationConvert.DurationUnit.MICRO;
import static loghub.processors.DurationConvert.DurationUnit.MILLI;
import static loghub.processors.DurationConvert.DurationUnit.NANO;
import static loghub.processors.DurationConvert.DurationUnit.SECOND;
import static loghub.processors.DurationConvert.DurationUnit.SECOND_FLOAT;

public class TestDurationConvert {

    private final EventsFactory factory = new EventsFactory();

    static private final Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    private Object process(Object value, DurationConvert.DurationUnit in, DurationConvert.DurationUnit out) throws ProcessorException {
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
    }

    @Test
    public void testFailure() {
        ProcessorException ex = Assert.assertThrows(ProcessorException.class, () -> process("a", MILLI, NANO));
        Assert.assertEquals("Field with path \"[field]\" invalid: Can't scan period a", ex.getMessage());
        ProcessorException ex2 = Assert.assertThrows(ProcessorException.class, () -> process(Instant.ofEpochSecond(0), MILLI, NANO));
        Assert.assertEquals("Field with path \"[field]\" invalid: Can't scan period 1970-01-01T00:00:00Z", ex2.getMessage());
    }

}
