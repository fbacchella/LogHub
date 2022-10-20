package loghub;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.Mocker;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;
import loghub.processors.Identity;

public class TestCriticalFailure {

    private static Logger logger ;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @After
    public void endJmx() {
        JmxService.stop();
    }

    @Test(timeout=10000)
    public void test() throws ConfigException, IOException, InterruptedException{
        String confile = "pipeline[newpipe] {}";

        Properties props = Tools.loadConf(new StringReader(confile));
        Start runner = new Start();
        runner.setCanexit(false);
        runner.launch(props);
        Event ev = Mocker.getMock();
        doThrow(new OutOfMemoryError()).when(ev).next();
        when(ev.getCurrentPipeline()).thenReturn("newpipe");
        props.mainQueue.add(ev);

        props.eventsprocessors.stream().forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                Assert.fail();
            }
        });
    }

    @Test(timeout=10000)
    public void testlatter() throws ConfigException, IOException, InterruptedException, ProcessorException{
        String confile = "pipeline[newpipe] {}";

        Properties props = Tools.loadConf(new StringReader(confile));
        Start runner = new Start();
        runner.setCanexit(false);
        runner.launch(props);
        Event ev = Mocker.getMock();
        doThrow(new StackOverflowError()).when(ev).process(any());
        when(ev.getCurrentPipeline()).thenReturn("newpipe");
        when(ev.next()).thenReturn(new Identity());
        CountDownLatch latch = new CountDownLatch(2);
        doAnswer(i -> {
            latch.countDown();
            return null;
        }).when(ev).end();
        AtomicReference<PipelineStat> arps = new AtomicReference<>();
        AtomicReference<Throwable> art = new AtomicReference<>();
        doAnswer(i -> {
            arps.set(i.getArgument(0));
            art.set(i.getArgument(1));
            System.out.println();
            latch.countDown();
            return null;
        }).when(ev).doMetric(any(), any());
        props.mainQueue.add(ev);
        latch.await();
        props.eventsprocessors.stream().forEach(EventsProcessor::stopProcessing);
        Assert.assertTrue(art.get() instanceof StackOverflowError);
        Assert.assertEquals(Stats.PipelineStat.EXCEPTION, arps.get());
    }

}
