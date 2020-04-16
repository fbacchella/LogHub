package loghub.processors;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.DefaultPromise;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Stats;
import loghub.ThreadBuilder;
import loghub.Tools;

public class TestPausingEvent {


    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.SleepingProcessor", "loghub.EventsProcessor");
    }

    private static class PausingPromise extends DefaultPromise<Object> {
        PausingPromise() {
            super(new DefaultEventLoop());
        }
    }

    PausingPromise future = new PausingPromise();
    Runnable todo;
    BiFunction<Event, Object, Boolean> onsucces;
    BiFunction<Event, Exception, Boolean> onexception;

    private class SleepingProcessor extends Processor implements AsyncProcessor<Object> {

        @Override
        public boolean processCallback(Event event, Object content)
                        throws ProcessorException {
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
        public boolean process(Event event) throws ProcessorException {
            ThreadBuilder.get().setTask(() -> {
                try {
                    Thread.sleep(200);
                    todo.run();
                } catch (InterruptedException e) {
                }
            })
            .build(true);
            ;
            throw new ProcessorException.PausedEventException(event, future);
        }
    }

    @Test(timeout=1000)
    public void success() throws ProcessorException, InterruptedException {
        long started = Instant.now().toEpochMilli();
        logger.debug("starting");
        todo = () -> future.setSuccess(this);
        onsucces = (e, v) -> {e.put("message", v); return true;};
        Event e = Tools.getEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        long end = Instant.now().toEpochMilli();
        Assert.assertTrue(String.format("slept for %d ms",  end - started), end > started + 200);
        Assert.assertEquals(this, e.get("message"));
    }

    @Test(timeout=1000)
    public void failed() throws ProcessorException, InterruptedException {
        Stats.reset();
        todo = () -> future.setSuccess(Boolean.TRUE);
        onsucces = (e, v) -> {return false;};
        Event e = Tools.getEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 1");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }


    @Test(timeout=1000)
    public void exceptionFalse() throws ProcessorException, InterruptedException {
        Stats.reset();
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> Boolean.FALSE;
        Event e = Tools.getEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 1");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }

    @Test(timeout=2000)
    public void exceptionException() throws ProcessorException, InterruptedException {
        Stats.reset();
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> {try {
            throw e.buildException("got it", x);
        } catch (ProcessorException ex) {
            throw new RuntimeException(ex);
        }};
        Event e = Tools.getEvent();
        SleepingProcessor sp = new SleepingProcessor();
        Groovy gp = new Groovy();
        gp.setScript("event.a = 1");
        sp.setException(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(1, e.get("a"));
    }

}
