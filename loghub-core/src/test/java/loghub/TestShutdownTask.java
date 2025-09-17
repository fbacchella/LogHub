package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public class TestShutdownTask {

    @Rule
    public ZMQFactory tctxt = new ZMQFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testShutdown() throws ConfigException, IOException, InterruptedException {
        Properties conf = Tools.loadConf("test.conf", false);
        List<Future<Boolean>> results;
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        results = new ArrayList<>(conf.pipelines.size());
        conf.pipelines.forEach(p -> p.configure(conf, executor, results));
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        boolean configured;
        configured = results.stream().map(this::futureGet).reduce((b1, b2) -> b1 && b2).orElse(false);
        Assert.assertTrue(configured);
        Receiver<?, ?>[] receivers = conf.receivers.toArray(Receiver[]::new);
        EventsProcessor[] eventProcessors = conf.eventsprocessors.toArray(EventsProcessor[]::new);
        Sender[] senders = conf.senders.toArray(Sender[]::new);
        ShutdownTask.configuration()
                    .eventProcessors(eventProcessors)
                    .loghubtimer(conf.timer)
                    .startTime(System.nanoTime())
                    .repositories(conf.eventsRepositories())
                    .dumpStats(false)
                    .eventProcessors(eventProcessors)
                    .systemd(SystemdHandler.nope())
                    .receivers(receivers)
                    .senders(senders)
                    .terminator(conf.terminator())
                    .register();
        ShutdownTask.shutdown();
        Assert.assertEquals(List.of(), Arrays.stream(receivers).filter(Objects::nonNull).toList());
        Assert.assertEquals(List.of(), Arrays.stream(eventProcessors).filter(Objects::nonNull).toList());
        Assert.assertEquals(List.of(), Arrays.stream(senders).filter(Objects::nonNull).toList());
    }

    boolean futureGet(Future<Boolean> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

}
