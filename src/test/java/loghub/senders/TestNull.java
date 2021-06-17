package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.encoders.Encoder;
import loghub.encoders.StringField;

public class TestNull {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.Null", "loghub.encoders.StringField");
    }

    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

    private Null send(Consumer<Null.Builder> prepare, Encoder encoder) throws InterruptedException {
        Null.Builder nb = Null.getBuilder();
        nb.setEncoder(encoder);
        prepare.accept(nb);
        Null nullsender = nb.build();
        nullsender.setInQueue(queue);

        Assert.assertTrue(nullsender.configure(new Properties(Collections.emptyMap())));
        nullsender.start();

        Event ev = Event.emptyEvent(new BlockingConnectionContext());
        ev.put("message", 1);
        queue.add(ev);
        ConnectionContext<Semaphore> ctxt = ev.getConnectionContext();
        ctxt.getLocalAddress().acquire();
        return nullsender;
    }

    @Test(timeout=2000)
    public void testNoEncode() throws InterruptedException {
        long start = System.nanoTime();
        send(b -> {
            b.setEncode(false);
            b.setBatchSize(10);
            b.setFlushInterval(1);
        }, (Encoder) null);
        Assert.assertTrue(queue.isEmpty());
        long duration = System.nanoTime() - start;
        Assert.assertTrue(duration > TimeUnit.SECONDS.toNanos(1) && duration < TimeUnit.SECONDS.toNanos(2));
    }

    @Test
    public void testEncode() throws InterruptedException {
        StringField.Builder builder1 = StringField.getBuilder();
        builder1.setFormat("${message%s}");
        StringField sf = builder1.build();

        send(b -> b.setEncode(true), sf);
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testFailing() throws InterruptedException {
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, () -> send(b -> b.setEncode(true), (Encoder) null));
        Assert.assertEquals("Encoding requested, but no encoder given", ex.getMessage());
    }

    @Test
    public void testBeans() throws ClassNotFoundException, IntrospectionException, InvocationTargetException {
        BeanChecks.beansCheck(logger, "loghub.senders.Null"
                              , BeanInfo.build("workers", Integer.TYPE)
                              , BeanInfo.build("batchSize", Integer.TYPE)
                              , BeanInfo.build("flushInterval", Integer.TYPE)
                              , BeanInfo.build("encode", Boolean.TYPE)
                             );
    }

}
