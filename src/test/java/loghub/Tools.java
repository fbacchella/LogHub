package loghub;

import java.io.IOException;
import java.io.Reader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Assert;

import io.netty.util.concurrent.Future;
import loghub.EventsProcessor;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class Tools {

    static public void configure() throws IOException {
        Locale.setDefault(new Locale("POSIX"));
        System.getProperties().setProperty("java.awt.headless","true");
        System.setProperty("java.io.tmpdir", "tmp");
        LogUtils.configure();
    }

    public static Properties loadConf(Reader config) throws ConfigException, IOException {
        Properties props = Configuration.parse(config);

        for(Pipeline pipe: props.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(props));
        }

        return props;
    }

    public static Properties loadConf(String configname, boolean dostart) throws ConfigException, IOException {
        String conffile = Configuration.class.getClassLoader().getResource(configname).getFile();
        Properties props = Configuration.parse(conffile);

        for(Pipeline pipe: props.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(props));
        }

        return props;
    }

    public static Properties loadConf(String configname) throws ConfigException, IOException {
        return loadConf(configname, true);
    }

    public static Event getEvent() {
        return new EventInstance(ConnectionContext.EMPTY);
    }

    public static void runProcessing(Event sent, Pipeline pipe, Properties props) throws ProcessorException {
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
        sent.inject(pipe, props.mainQueue);
        Processor processor;
        while ((processor = sent.next()) != null) {
            ep.process(sent, processor);
        }
    }

    public static class ProcessingStatus {
        public BlockingQueue<Event> mainQueue;
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
        Properties props = new Properties(Collections.emptyMap());
        ProcessingStatus ps = new ProcessingStatus();
        ps.mainQueue = props.mainQueue;
        ps.status = new ArrayList<>();
        ps.repository = props.repository;
        Pipeline pipe = new Pipeline(steps, pipename, null);

        Map<String, Pipeline> namedPipeLine = Collections.singletonMap(pipename, pipe);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, namedPipeLine, 100, props.repository);
        steps.forEach( i -> Assert.assertTrue(i.configure(props)));
        prepare.accept(props, steps);
        sent.inject(pipe, props.mainQueue);
        Event toprocess;
        Processor processor;
        // Process all the events, will hang forever is it don't finish
        try {
            while ((toprocess = props.mainQueue.poll(5, TimeUnit.SECONDS)) != null) {
                while ((processor = toprocess.next()) != null) {
                    EventsProcessor.ProcessingStatus status = ep.process(toprocess, processor);
                    ps.status.add(status.name());
                    if (status != loghub.EventsProcessor.ProcessingStatus.SUCCESS) {
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
        }
        return ps;
    }

    public static int tryGetPort() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(0);
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            return -1;
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }
    }

    public static boolean isInMaven() {
        return "true".equals(System.getProperty("maven.surefire", "false"));
    }
    
    public static final Function<Date, Boolean> isRecent = i -> { long now = new Date().getTime() ; return (i.getTime() > (now - 60000)) && (i.getTime() < (now + 60000));};

}
