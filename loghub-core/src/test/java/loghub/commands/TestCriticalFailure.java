package loghub.commands;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.SystemdHandler;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.Mocker;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;
import loghub.processors.Identity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class TestCriticalFailure {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @After
    public void endJmx() {
        JmxService.stop();
    }

    @Test(timeout=10000)
    public void test() throws ConfigException, IOException {
        String confile = String.format("pipeline[newpipe] {} hprofDumpPath:\"%s/loghub.hprof\"", folder.getRoot());

        Properties props = Tools.loadConf(new StringReader(confile));
        Launch runner = new Launch();
        runner.launch(props, SystemdHandler.nope());
        Event ev = Mocker.getMock();
        doThrow(new OutOfMemoryError()).when(ev).next();
        when(ev.getCurrentPipeline()).thenReturn("newpipe");
        props.mainQueue.add(ev);

        props.eventsprocessors.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                Assert.fail();
            }
        });
        Assert.assertTrue(Files.exists(folder.getRoot().toPath().resolve("loghub.hprof")));
    }

    @Test(timeout=10000)
    public void testlatter() throws ConfigException, IOException, InterruptedException, ProcessorException {
        String confile = "pipeline[newpipe] {}";

        Properties props = Tools.loadConf(new StringReader(confile));
        Launch runner = new Launch();
        runner.launch(props, SystemdHandler.nope());
        Event ev = Mocker.getMock();
        doThrow(new StackOverflowError()).when(ev).process(any());
        when(ev.getCurrentPipeline()).thenReturn("newpipe");
        when(ev.next()).thenReturn(new Identity());
        when(ev.getPipelineLogger()).thenReturn(LogManager.getLogger("loghub.Pipeline.newpipe"));
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
            latch.countDown();
            return null;
        }).when(ev).doMetric(any(), any());
        props.mainQueue.add(ev);
        latch.await();
        props.eventsprocessors.forEach(EventsProcessor::stopProcessing);
        Assert.assertTrue(art.get() instanceof StackOverflowError);
        Assert.assertEquals(Stats.PipelineStat.EXCEPTION, arps.get());
    }

}
