package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Timer;

import loghub.BeanChecks;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

public class TestRateLimiter {

    private static Logger logger;
    private static final EventsFactory factory = new EventsFactory();
    private static final Duration testDuration = Duration.ofSeconds(5);

    private final AtomicInteger sent = new AtomicInteger();
    private final AtomicInteger received = new AtomicInteger();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.RateLimiter");
    }

    private void withinPercentage(int expected, int actual, double percent) {
        double lower = expected * (1 - percent / 100.0);
        double upper = expected * (1 + percent / 100.0);
        Assert.assertTrue("Expecting %d, got %d".formatted(expected, actual),actual >= lower && actual <= upper);
    }

    private void runFlow(Consumer<RateLimiter.Builder> configurator, int expected) throws IOException, InterruptedException {
        Properties props = Tools.loadConf(new StringReader("queueDepth: 1 queueWeight: 2 pipeline[main] {} output $main | {}"));
        RateLimiter.Builder builder = RateLimiter.getBuilder();
        configurator.accept(builder);
        RateLimiter rl = builder.build();
        Assert.assertTrue(rl.configure(props));
        Pipeline pipe = new Pipeline(Collections.singletonList(rl), "main", null);
        Thread t = ThreadBuilder.get().setTask(() -> {
            while (!Thread.interrupted()) {
                Event ev = factory.newEvent();
                ev.inject(pipe, props.mainQueue, true);
                sent.incrementAndGet();
            }
        }).setVirtual(true).build(true);

        Thread consumer = ThreadBuilder.get().setTask(() -> {
            try {
                while (!Thread.interrupted()) {
                    props.outputQueues.get("main").poll(100, TimeUnit.MILLISECONDS);
                    received.incrementAndGet();
                }
            } catch (InterruptedException e) {
                // just exit
            }
        }).setVirtual(true).build(true);

        Map<String, Pipeline> namedPipeLine = Collections.singletonMap("main,", pipe);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, namedPipeLine, 100, props.repository);
        ep.start();
        Thread.sleep(testDuration.toMillis());
        t.interrupt();
        consumer.interrupt();
        ep.interrupt();
        t.join();
        consumer.join();
        ep.join();
        logger.debug("Send: {}", sent::get);
        logger.debug("Received: {}", received::get);
        logger.debug("Dropped: {}", Stats::getDropped);
        logger.debug("Processed in pipeline main: {}", () -> Stats.getMetric("main", "timer", Timer.class).getCount());
        withinPercentage(expected, received.get() / (int) testDuration.toSeconds(), 5);
        Assert.assertTrue(Integer.toString(sent.get() - (received.get() + (int) Stats.getDropped())), (sent.get() - (received.get() + Stats.getDropped())) < 5);
    }

    @Test
    public void testBaseRate() throws InterruptedException, ConfigException, IOException {
        runFlow(b -> b.setRate(100), 120);
        logger.debug("Nominal requested rate is {}", () -> (float) received.get() / testDuration.toSeconds());
        Assert.assertEquals(0, Stats.getDropped());
    }

    @Test
    public void testBurstRate() throws InterruptedException, ConfigException, IOException {
        runFlow(b -> {
            b.setRate(100);
            b.setBurstRate(200);
        }, 140);
        logger.debug("Burst requested rate is {}", () -> (float) received.get() /testDuration.toSeconds());
        Assert.assertEquals(0, Stats.getDropped());
    }

    @Test
    public void testRateDropping() throws InterruptedException, ConfigException, IOException {
        runFlow(b -> {
            b.setRate(100);
            b.setDropping(true);
        }, 120);
        logger.debug("Dropping requested rate is {}", () -> (float) received.get() / testDuration.toSeconds());
        Assert.assertNotEquals(0, Stats.getDropped());
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.RateLimiter"
                              , BeanChecks.BeanInfo.build("rate", long.class)
                              , BeanChecks.BeanInfo.build("burstRate", long.class)
                              , BeanChecks.BeanInfo.build("dropping", boolean.class)
        );
    }

}
