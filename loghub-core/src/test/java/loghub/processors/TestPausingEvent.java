package loghub.processors;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

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
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

public class TestPausingEvent {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.SleepingProcessor", "loghub.EventsProcessor");
    }

    private static class PausingPromise extends DefaultPromise<Object> {
        PausingPromise() {
            super(new DefaultEventLoop());
        }
    }

    final PausingPromise future = new PausingPromise();
    Runnable todo;
    BiFunction<Event, Object, Boolean> onsucces;
    BiFunction<Event, Exception, Boolean> onexception;

    private class SleepingProcessor extends Processor implements AsyncProcessor<Object, Promise<Object>> {

        @Override
        public boolean processCallback(Event event, Object content) {
            logger.debug("Will process {} with content {}", event, content);
            return onsucces.apply(event, content);
        }

        @Override
        public boolean manageException(Event event, Exception e)
                        throws ProcessorException {
            try {
                return onexception.apply(event, e);
            } catch (RuntimeException ex) {
                throw (ProcessorException) ex.getCause();
            }
        }

        @Override
        public int getTimeout() {
            return 1;
        }

        @Override
        public boolean process(Event event) {
            ThreadBuilder.get().setTask(() -> {
                try {
                    Thread.sleep(200);
                    todo.run();
                } catch (InterruptedException e) {
                    // No nothing
                }
            })
            .build(true);

            throw new AsyncProcessor.PausedEventException(future);
        }

        @Override
        public BiConsumer<Event, Promise<Object>> getTimeoutHandler() {
            return (e, p) -> p.setFailure(new TimeoutException());
        }
    }

    @Test(timeout=2000)
    public void success() throws InterruptedException {
        long started = Instant.now().toEpochMilli();
        logger.debug("starting");
        todo = () -> future.setSuccess(this);
        onsucces = (e, v) -> {e.put("message", v); return true;};
        Event e = factory.newEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        long end = Instant.now().toEpochMilli();
        Assert.assertTrue(String.format("slept for %d ms",  end - started), end > started + 200);
        Assert.assertEquals(this, e.get("message"));
    }

    @Test(timeout=2000)
    public void failed() throws InterruptedException {
        Stats.reset();
        todo = () -> future.setSuccess(Boolean.TRUE);
        onsucces = (e, v) -> false;
        Event e = factory.newEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Etl assign = Etl.Assign.of(VariablePath.of("a"), new Expression(1));
        sp.setFailure(assign);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }


    @Test(timeout=2000)
    public void exceptionFalse() throws InterruptedException {
        Stats.reset();
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> Boolean.FALSE;
        Event e = factory.newEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Etl assign = Etl.Assign.of(VariablePath.of("a"), new Expression(1));
        sp.setFailure(assign);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }

    @Test(timeout=2000)
    public void exceptionException() throws InterruptedException {
        Stats.reset();
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> {try {
            throw e.buildException("got it", x);
        } catch (ProcessorException ex) {
            throw new RuntimeException(ex);
        }};
        Event e = factory.newEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Etl assign = Etl.Assign.of(VariablePath.of("a"), new Expression(1));
        sp.setException(assign);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> { /* empty */ });
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }

}
