package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.EventsProcessor;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestRateLimiter {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.RateLimiter");
    }

    @Test
    public void testRate() throws InterruptedException, ConfigException, IOException {
        Properties props = Tools.loadConf(new StringReader("queueDepth: 10 queueWeigth: 2"));
        AtomicInteger ai = new AtomicInteger();
        RateLimiter.Builder builder = RateLimiter.getBuilder();
        builder.setRate(100);
        builder.setBurstRate(-1);
        RateLimiter rl = builder.build();
        Assert.assertTrue(rl.configure(props));
        Pipeline pipe = new Pipeline(Collections.singletonList(rl), "main", null);
        Thread t = ThreadBuilder.get().setTask(() -> {
            while (!Thread.interrupted()) {
                Event ev = factory.newEvent(ConnectionContext.EMPTY);
                ev.put("count", ai.incrementAndGet());
                ev.inject(pipe, props.mainQueue, true);
            }
        }).build(true);

        Map<String, Pipeline> namedPipeLine = Collections.singletonMap("main,", pipe);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, namedPipeLine, 100, props.repository);
        ep.start();
        Thread.sleep(5000);
        t.interrupt();
        ep.interrupt();
        System.err.println(ai.get());
        Assert.assertTrue(ai.get() < 500);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.RateLimiter"
                              , BeanChecks.BeanInfo.build("rate", Integer.TYPE)
                              , BeanChecks.BeanInfo.build("burstRate", Integer.TYPE)
                              , BeanChecks.BeanInfo.build("key", Expression.class)
        );
    }

}
