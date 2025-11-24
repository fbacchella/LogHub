package loghub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.junit.Assert;

import com.beust.jcommander.JCommander;

import io.netty.util.concurrent.Future;
import loghub.commands.Parser;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.processors.UnwrapEvent;
import loghub.processors.WrapEvent;
import loghub.security.ssl.MultiKeyStoreProvider;
import loghub.senders.BlockingConnectionContext;

import static loghub.EventsProcessor.ProcessingStatus.DISCARD;
import static loghub.EventsProcessor.ProcessingStatus.DROPED;
import static loghub.EventsProcessor.ProcessingStatus.ERROR;

public class Tools {

    public static void configure() {
        Locale.setDefault(Locale.of("POSIX"));
        LogUtils.configure();
        Stats.reset();
    }

    public static KeyStore getDefaultKeyStore() {
        return getKeyStore(Tools.class.getResource("/loghub.p12").getFile());
    }

    public static KeyStore getKeyStore(String... paths) {
        try {
            MultiKeyStoreProvider.SubKeyStore param = new MultiKeyStoreProvider.SubKeyStore();
            for (String path : paths) {
                param.addSubStore(path);
            }
            KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
            ks.load(param);
            return ks;
        } catch (KeyStoreException | NoSuchProviderException | IOException | NoSuchAlgorithmException |
                 CertificateException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties loadConf(Reader config) throws ConfigException, IOException {
        Properties props = Configuration.parse(config);

        Helpers.parallelStartProcessor(props);

        props.receivers.forEach(r -> Assert.assertTrue("configuration failed", r.configure(props)));

        return props;
    }

    public static Properties loadConf(String configname, boolean dostart) throws ConfigException, IOException {
        String conffile = Configuration.class.getClassLoader().getResource(configname).getFile();
        Properties props = Configuration.parse(conffile);
        props.receivers.forEach(r -> r.configure(props));
        props.senders.forEach(s -> s.configure(props));
        Helpers.parallelStartProcessor(props);
        return props;
    }

    public static Properties loadConf(String configname) throws ConfigException, IOException {
        return loadConf(configname, true);
    }

    public static void runProcessing(Event sent, Pipeline pipe, Properties props) {
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
        sent.inject(pipe, props.mainQueue, false);
        Processor processor;
        while ((processor = sent.next()) != null) {
            if (processor instanceof WrapEvent) {
                sent = sent.wrap(processor.getPathArray());
            } else if (processor instanceof UnwrapEvent) {
                sent = sent.unwrap();
            } else {
                EventsProcessor.ProcessingStatus status = ep.process(sent, processor);
                if (status == DROPED) {
                    sent.drop();
                    break;
                } else if (status == ERROR || status == DISCARD) {
                    sent.end();
                    break;
                }
            }
        }
    }

    public static class ProcessingStatus {
        public PriorityBlockingQueue mainQueue;
        public List<String> status;
        public EventsRepository<Future<?>> repository;
        @Override
        public String toString() {
            return mainQueue + " / " + status + " / " + repository;
        }
    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps) {
        return runProcessing(sent, pipename, steps, (i, j) -> { });
    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps,
            BiConsumer<Properties, List<Processor>> prepare) {
        return runProcessing(sent, pipename, steps, prepare, new Properties(Collections.emptyMap()));
    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps,
            BiConsumer<Properties, List<Processor>> prepare, Properties props) {
        ProcessingStatus ps = new ProcessingStatus();
        ps.mainQueue = props.mainQueue;
        ps.status = new ArrayList<>();
        ps.repository = props.repository;
        Pipeline pipe = new Pipeline(steps, pipename, null);
        Stats.registerPipeline(pipename);
        Map<String, Pipeline> namedPipeLine = Collections.singletonMap(pipename, pipe);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, namedPipeLine, 100,
                props.repository);
        steps.forEach(i -> Assert.assertTrue(i.configure(props)));
        prepare.accept(props, steps);
        sent.inject(pipe, props.mainQueue, false);
        Event toprocess;
        Processor processor;
        // Process all the events, will hang forever if it doesn't finish
        try {
            while ((toprocess = props.mainQueue.poll(5, TimeUnit.SECONDS)) != null) {
                while ((processor = toprocess.next()) != null) {
                    EventsProcessor.ProcessingStatus status = ep.process(toprocess, processor);
                    ps.status.add(status.name());
                    if (status != loghub.EventsProcessor.ProcessingStatus.CONTINUE) {
                        toprocess = null;
                        break;
                    }
                }
                if (toprocess != null) {
                    ps.mainQueue.put(toprocess);
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return ps;
    }

    public static int tryGetPort() {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    public static boolean isInMaven() {
        return System.getProperty("surefire.real.class.path") != null || System.getProperty(
                "surefire.test.class.path") != null;
    }

    public static final Function<Date, Boolean> isRecent = i -> {
        long now = new Date().getTime();
        return (i.getTime() > (now - 60000)) && (i.getTime() < (now + 60000));
    };

    public static Expression parseExpression(String exp) {
        return ConfigurationTools.unWrap(exp, RouteParser::expression);
    }

    public static Object resolveExpression(String exp) {
        return ConfigurationTools.unWrap(exp, RouteParser::expression);
    }

    public static Object evalExpression(String exp, Event ev) throws ProcessorException {
        Object resolved = ConfigurationTools.unWrap(exp, RouteParser::expression);
        if (resolved instanceof Expression) {
            return ((Expression) resolved).eval(ev);
        } else {
            return resolved;
        }
    }

    public static Object evalExpression(String exp) throws ProcessorException {
        return evalExpression(exp, null);
    }

    public static Event processEventWithPipeline(EventsFactory factory, String configuration, String pipelineName,
            Consumer<Event> populateEvent) throws IOException, InterruptedException {
        Properties props = Tools.loadConf(new StringReader(configuration));
        return processEventWithPipeline(factory, props, pipelineName, populateEvent);
    }

    public static Event processEventWithPipeline(EventsFactory factory, Properties props, String pipelineName,
            Consumer<Event> populateEvent) throws InterruptedException {
        return processEventWithPipeline(factory, props, pipelineName, populateEvent, () -> {});
    }

    public static Event processEventWithPipeline(EventsFactory factory, Properties props, String pipelineName,
            Consumer<Event> populateEvent, Runnable postRun) throws InterruptedException {
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, 100,
                props.repository);
        ep.start();
        BlockingConnectionContext ctx = new BlockingConnectionContext();
        Event ev = factory.newEvent(ctx);
        populateEvent.accept(ev);
        ev.inject(props.namedPipeLine.get(pipelineName), props.mainQueue, true);
        boolean computed = ctx.getLocalAddress().tryAcquire(5, TimeUnit.SECONDS);
        Assert.assertTrue(computed);
        postRun.run();
        ep.interrupt();
        ep.join();
        return ev;
    }

    public static void executeCmd(Parser parser, UnaryOperator<String[]> transform, String expected, int expectedStatus,
            String... args) throws IOException {
        String[] newargs = transform.apply(args);
        JCommander jcom = parser.parse(newargs);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assert.assertEquals(expectedStatus, status);
            Assert.assertEquals(expected, bos.toString(StandardCharsets.UTF_8));
        }
    }

    public static void executeCmd(Parser parser, String expected, int expectedStatus, String... args)
            throws IOException {
        executeCmd(parser, a -> a, expected, expectedStatus, args);
    }

}
