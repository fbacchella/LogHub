package loghub.processors;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import loghub.AsyncProcessor;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestFieldsAsynchronous {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.EventsProcessor", "loghub.Event", "loghub.EventsRepository", "io.netty.util.HashedWheelTimer");
    }

    private static class PausingPromise extends DefaultPromise<TestFieldsAsynchronous> {
        private static final DefaultEventLoop executor = new DefaultEventLoop();
        PausingPromise() {
            super(executor);
        }
    }

    Consumer<PausingPromise> todo;
    BiFunction<Event, Exception, Boolean> onexception;
    BiFunction<Event, TestFieldsAsynchronous, Object> transform;

    public class Builder extends AsyncFieldsProcessor.Builder<SleepingProcessor, TestFieldsAsynchronous, Promise<TestFieldsAsynchronous>> {
        @Override
        public SleepingProcessor build() {
            this.setQueueDepth(10);
            return new SleepingProcessor(this);
        }
    }

    private class SleepingProcessor extends AsyncFieldsProcessor<TestFieldsAsynchronous, Promise<TestFieldsAsynchronous>> {
        public SleepingProcessor(Builder<SleepingProcessor,TestFieldsAsynchronous, Promise<TestFieldsAsynchronous>> builder) {
            super(builder);
        }

        @Override
        public Object asyncProcess(Event event, TestFieldsAsynchronous content) {
            return transform.apply(event, content);
        }

        @Override
        public boolean manageException(Event event, Exception e,
                                       VariablePath destination)
                                                       throws ProcessorException {
            try {
                return onexception.apply(event, e);
            } catch (Exception ex) {
                throw event.buildException("", ex);
            }
        }

        @Override
        public Object fieldFunction(Event event, Object value) {
            PausingPromise f = new PausingPromise();
            ThreadBuilder.get().setTask(() -> {
                try {
                    Thread.sleep(200);
                    todo.accept(f);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .build(true);
            throw new AsyncProcessor.PausedEventException(f);
        }

        @Override
        public BiConsumer<Event, Promise<TestFieldsAsynchronous>> getTimeoutHandler() {
            return (e, v) -> v.setFailure(new TimeoutException());
        }
    }

    @Test(timeout=2000)
    public void success() throws ProcessorException, InterruptedException {
        long started = Instant.now().toEpochMilli();
        logger.debug("starting");
        todo = (v) -> {
            if ( ! v.isDone()) {
                v.setSuccess(TestFieldsAsynchronous.this);
            }
        };
        transform = (e, v) -> v.getClass().getCanonicalName();
        Event e = factory.newEvent();
        e.put("a", 1);
        e.put("b", 2);
        Builder builder = new Builder();
        builder.setFields(new String[] {"a", "b"});
        SleepingProcessor sp = builder.build();
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        long end = Instant.now().toEpochMilli();
        Assert.assertTrue(String.format("slept for %d ms",  end - started), end > started + 200);
        Assert.assertEquals(getClass().getCanonicalName(), e.get("a"));
        Assert.assertEquals(getClass().getCanonicalName(), e.get("b"));
    }

    @Test(timeout=1000)
    public void successWithDestination() throws ProcessorException, InterruptedException {
        long started = Instant.now().toEpochMilli();
        logger.debug("starting");
        todo = (v) -> {
            if ( ! v.isDone()) {
                v.setSuccess(this);
            }
        };
        transform = (e, v) -> v.getClass().getCanonicalName();
        Event e = factory.newEvent();
        e.put("a", 1);
        e.put("b", 2);
        Builder builder = new Builder();
        builder.setFields(new String[] {"a", "b"});
        builder.setDestinationTemplate(new VarFormatter("${field}_done"));
        SleepingProcessor sp = builder.build();
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        long end = Instant.now().toEpochMilli();
        Assert.assertTrue(String.format("slept for %d ms",  end - started), end > started + 200);
        Assert.assertEquals(getClass().getCanonicalName(), e.get("a_done"));
        Assert.assertEquals(getClass().getCanonicalName(), e.get("b_done"));
    }

    @Test(timeout=1000)
    public void failed() throws ProcessorException, InterruptedException {
        todo = (v) -> v.setSuccess(this);
        transform = (e, v) -> FieldsProcessor.RUNSTATUS.FAILED;
        Event e = factory.newEvent();
        e.put("a", 1);
        Builder builder = new Builder();
        builder.setField(VariablePath.of("a"));
        SleepingProcessor sp = builder.build();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }

    @Test(timeout=5000)
    public void timeout() throws ProcessorException, InterruptedException {
        todo = (v) -> {
            try {
                Thread.sleep(2000);
                v.setSuccess(this);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        };
        transform = (e, v) -> v.getClass().getCanonicalName();
        Event e = factory.newEvent();
        e.put("a", 1);
        Builder builder = new Builder();
        builder.setTimeout(1);
        builder.setField(VariablePath.of("a"));
        SleepingProcessor sp = builder.build();
        Groovy gp = new Groovy();
        gp.setScript("event.failure = true");
        sp.setFailure(gp);
        List<Processor> processors = new ArrayList<>(2);

        Groovy gp2 = new Groovy();
        gp2.setScript("event.b = true");

        processors.add(sp);
        processors.add(gp2);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", processors, (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(true, e.get("failure"));
        Assert.assertEquals(true, e.get("b"));
        Assert.assertEquals(1, e.get("a"));
    }


    @Test(timeout=1000)
    public void exceptionFalse() throws ProcessorException, InterruptedException {
        todo = (v) -> v.setFailure(new RuntimeException());
        onexception = (e, x) -> Boolean.FALSE;
        transform = (e, v) -> v.getClass().getCanonicalName();
        Event e = factory.newEvent();
        e.put("a", 1);
        Builder builder = new Builder();
        builder.setField(VariablePath.of("a"));
        SleepingProcessor sp = builder.build();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }

    @Test(timeout=1000)
    public void exceptionException() throws ProcessorException, InterruptedException {
        todo = (v) -> v.setFailure(new RuntimeException());
        transform = (e, v) -> v.getClass().getCanonicalName();
        onexception = (e, x) -> {
            try {
                throw e.buildException("got it", x);
            } catch (ProcessorException ex) {
                throw new RuntimeException(ex);
            }
        };
        Event e = factory.newEvent();
        e.put("a", 1);
        Builder builder = new Builder();
        builder.setField(VariablePath.of("a"));
        SleepingProcessor sp = builder.build();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setException(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }

}
