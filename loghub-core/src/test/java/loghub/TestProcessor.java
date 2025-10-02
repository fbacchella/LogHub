package loghub;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Meter;

import loghub.EventsProcessor.ProcessingStatus;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.metrics.Stats.EventExceptionDescription;
import loghub.processors.Drop;
import loghub.processors.Identity;

public class TestProcessor {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    @Test
    public void testPath() {
        Processor p = new Processor() {

            @Override
            public boolean process(Event event) {
                return true;
            }

            @Override
            public String getName() {
                return null;
            }
        };
        p.setPath("a.b.c");
        Assert.assertEquals("Prefix don't match ", VariablePath.of("a", "b", "c"), p.getPathArray());
        p.setPath("");
        Assert.assertEquals("Prefix don't match ", VariablePath.EMPTY, p.getPathArray());
        p.setPath(".a");
        Assert.assertEquals("Prefix don't match ", VariablePath.parse(".a"), p.getPathArray());
    }

    @Test
    public void testIf() throws ProcessorException {
        Event e = factory.newEvent();

        Processor p = new Identity();

        p.setIf(new Expression(true));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(false));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(new Expression(0));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(new Expression(1));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(0.1));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression("bob"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(""));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));
    }

    private void runFailedProcessing(Processor p, List<ProcessingStatus> statuses) {
        Properties props = new Properties(Collections.emptyMap());
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, 100,
                props.repository);
        Event event = factory.newEvent();
        event.setTimestamp(Instant.ofEpochMilli(0));
        Pipeline pp = new Pipeline(List.of(p), "main", null);
        event.refill(pp);
        Processor nextP;
        int i = 0;
        while ((nextP = event.next()) != null) {
            ProcessingStatus status = ep.process(event, nextP);
            Assert.assertEquals(statuses.get(i++), status);
        }
    }

    @Test
    public void testDrop() {
        Processor p = new Drop();
        runFailedProcessing(p, List.of(ProcessingStatus.CONTINUE, ProcessingStatus.DROPED, ProcessingStatus.CONTINUE));
        Assert.assertEquals(1, Stats.getMetric(String.class, "dropped", Meter.class).getCount());
        Assert.assertEquals(1, Stats.getMetric("main", "dropped", Meter.class).getCount());
    }

    @Test
    public void testFailedProcessing() {
         Processor p = new Processor() {
            @Override
            public boolean process(Event event) throws ProcessorException {
                throw event.buildException("test failure", new RuntimeException("test failure"));
            }
            @Override
            public String getName() {
                return null;
            }

        };
        runFailedProcessing(p, List.of(ProcessingStatus.CONTINUE, ProcessingStatus.ERROR, ProcessingStatus.CONTINUE));
        Assert.assertEquals(1, Stats.getMetric(String.class, "failed", Meter.class).getCount());
        Assert.assertEquals(1, Stats.getMetric("main", "failed", Meter.class).getCount());
    }

    @Test
    public void testRuntimeProcessing() {
        Processor p = new Processor() {
            @Override
            public boolean process(Event event) {
                throw new RuntimeException("test failure");
            }
            @Override
            public String getName() {
                return null;
            }

        };
        runFailedProcessing(p, List.of(ProcessingStatus.CONTINUE, ProcessingStatus.ERROR, ProcessingStatus.CONTINUE));
        Assert.assertEquals(1, Stats.getMetric(String.class, "exception", Meter.class).getCount());
        Assert.assertEquals(1, Stats.getMetric("main", "exception", Meter.class).getCount());
    }

    @Test
    public void testOutOfMemory() {
        Processor p = new Processor() {
            @Override
            public boolean process(Event event) {
                throw new OutOfMemoryError("test failure");
            }
            @Override
            public String getName() {
                return null;
            }

        };
        runFailedProcessing(p, List.of(ProcessingStatus.CONTINUE, ProcessingStatus.ERROR, ProcessingStatus.CONTINUE));
    }

}
