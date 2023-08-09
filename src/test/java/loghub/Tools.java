package loghub;

import java.io.IOException;
import java.io.Reader;
import java.net.ServerSocket;
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
import java.util.function.Function;

import org.junit.Assert;

import io.netty.util.concurrent.Future;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.processors.UnwrapEvent;
import loghub.processors.WrapEvent;
import loghub.security.ssl.MultiKeyStoreProvider;

import static loghub.EventsProcessor.ProcessingStatus.DROPED;
import static loghub.EventsProcessor.ProcessingStatus.ERROR;

public class Tools {

    static public void configure() throws IOException {
        Locale.setDefault(new Locale("POSIX"));
        System.getProperties().setProperty("java.awt.headless","true");
        LogUtils.configure();
    }

    public static KeyStore getDefaultKeyStore() {
        return getKeyStore(Tools.class.getResource("/loghub.p12").getFile());
    }

    public static KeyStore getKeyStore(String... paths) {
        try {
            MultiKeyStoreProvider.SubKeyStore param = new MultiKeyStoreProvider.SubKeyStore();
            for(String path: paths) {
                param.addSubStore(path);
            }
            KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
            ks.load(param);
            return ks;
        } catch (KeyStoreException | NoSuchProviderException | IOException | NoSuchAlgorithmException | CertificateException ex) {
            throw  new RuntimeException(ex);
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

        Helpers.parallelStartProcessor(props);
        return props;
    }

    public static Properties loadConf(String configname) throws ConfigException, IOException {
        return loadConf(configname, true);
    }

    public static void runProcessing(Event sent, Pipeline pipe, Properties props) throws ProcessorException {
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
                } else if (status == ERROR) {
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
            return mainQueue + " / " +  status + " / " + repository;
        }

    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps) throws ProcessorException {
        return runProcessing(sent, pipename, steps, (i,j) -> {});
    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps, BiConsumer<Properties, List<Processor>> prepare) throws ProcessorException {
        return runProcessing(sent, pipename, steps, prepare, new Properties(Collections.emptyMap()));
    }

    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps, BiConsumer<Properties, List<Processor>> prepare, Properties props) throws ProcessorException {
        ProcessingStatus ps = new ProcessingStatus();
        ps.mainQueue = props.mainQueue;
        ps.status = new ArrayList<>();
        ps.repository = props.repository;
        Pipeline pipe = new Pipeline(steps, pipename, null);

        Map<String, Pipeline> namedPipeLine = Collections.singletonMap(pipename, pipe);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, namedPipeLine, 100, props.repository);
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
        return System.getProperty("surefire.real.class.path") != null || System.getProperty("surefire.test.class.path") != null;
    }

    public static final Function<Date, Boolean> isRecent = i -> { long now = new Date().getTime() ; return (i.getTime() > (now - 60000)) && (i.getTime() < (now + 60000));};

    public static Expression parseExpression(String exp) {
        return ConfigurationTools.unWrap(exp, RouteParser::expression);
    }

    public static  Object evalExpression(String exp, Event ev) throws Expression.ExpressionException, ProcessorException {
        return parseExpression(exp).eval(ev);
    }

    public static  Object evalExpression(String exp) throws Expression.ExpressionException, ProcessorException {
        return evalExpression(exp, null);
    }

}
