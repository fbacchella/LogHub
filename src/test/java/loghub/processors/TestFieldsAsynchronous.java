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
import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.Tools;

public class TestFieldsAsynchronous {


    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.EventsProcessor", "loghub.Event");
    }

    private static class PausingPromise extends DefaultPromise<TestFieldsAsynchronous> {
        PausingPromise() {
            super(new DefaultEventLoop());
        }
    }

    PausingPromise future = new PausingPromise();
    Runnable todo;
    BiFunction<Event, Exception, Boolean> onexception;
    BiFunction<Event, TestFieldsAsynchronous, Object> transform = (e, v) -> v.getClass().getCanonicalName();

    private class SleepingProcessor extends AsyncFieldsProcessor<TestFieldsAsynchronous> {

        @Override
        public Object asyncProcess(Event event, TestFieldsAsynchronous content)
                        throws ProcessorException {
            return transform.apply(event, content);
        }

        @Override
        public boolean manageException(Event event, Exception e,
                                       String[] destination)
                                                       throws ProcessorException {
            try {
                return onexception.apply(event, e);
            } catch (Exception ex) {
                if (ex instanceof ProcessorException) {
                    throw (ProcessorException) ex;
                } else {
                    throw event.buildException("", ex);
                }
            }
        }

        @Override
        public Object fieldFunction(Event event, Object value)
                        throws ProcessorException {
            ThreadBuilder.get().setRunnable(() -> {
                try {
                    Thread.sleep(200);
                    todo.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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
        todo = () -> {
            if ( ! future.isDone()) {
                future.setSuccess(this);
            }
        };
        Event e = Tools.getEvent();
        e.put("a", 1);
        e.put("b", 2);
        SleepingProcessor sp = new SleepingProcessor();
        sp.setFields(new String[] {"a", "b"});
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
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
        todo = () -> {
            if ( ! future.isDone()) {
                future.setSuccess(this);
            }
        };
        Event e = Tools.getEvent();
        e.put("a", 1);
        e.put("b", 2);
        SleepingProcessor sp = new SleepingProcessor();
        sp.setFields(new String[] {"a", "b"});
        sp.setDestination("${field}_done");
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        long end = Instant.now().toEpochMilli();
        Assert.assertTrue(String.format("slept for %d ms",  end - started), end > started + 200);
        Assert.assertEquals(getClass().getCanonicalName(), e.get("a_done"));
        Assert.assertEquals(getClass().getCanonicalName(), e.get("b_done"));
    }

    @Test(timeout=1000)
    public void failed() throws ProcessorException, InterruptedException {
        todo = () -> future.setSuccess(this);
        transform = (e, v) -> FieldsProcessor.RUNSTATUS.FAILED;
        Event e = Tools.getEvent();
        e.put("a", 1);
        SleepingProcessor sp = new SleepingProcessor();
        sp.setField(new String[] {"a"});
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }


    @Test(timeout=1000)
    public void exceptionFalse() throws ProcessorException, InterruptedException {
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> Boolean.FALSE;
        Event e = Tools.getEvent();
        e.put("a", 1);
        SleepingProcessor sp = new SleepingProcessor();
        sp.setField(new String[] {"a"});
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setFailure(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }

    @Test(timeout=1000)
    public void exceptionException() throws ProcessorException, InterruptedException {
        todo = () -> future.setFailure(new RuntimeException());
        onexception = (e, x) -> {
            try {
                throw e.buildException("got it", x);
            } catch (ProcessorException ex) {
                throw new RuntimeException(ex);
            }
        };
        Event e = Tools.getEvent();
        e.put("a", 1);
        SleepingProcessor sp = new SleepingProcessor();
        sp.setField(new String[] {"a"});
        Groovy gp = new Groovy();
        gp.setScript("event.a = 2");
        sp.setException(gp);
        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(sp), (i,j) -> {});
        e = status.mainQueue.take();
        Assert.assertEquals(2, e.get("a"));
    }

}
