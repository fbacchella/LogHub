package loghub;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Assert;

import io.netty.util.concurrent.Future;
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
        return new EventInstance();
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
        BlockingQueue<Event> mainQueue;
        List<Integer> status;
        @Override
        public String toString() {
            return mainQueue + " / " +  status;
        }
        
    }
    
    public static ProcessingStatus runProcessing(Event sent, String pipename, List<Processor> steps) throws ProcessorException {
        BlockingQueue<Event> mainQueue = new ArrayBlockingQueue<Event>(100);
        Map<String, BlockingQueue<Event>> outputQueues = Collections.emptyMap();
        Pipeline pipe = new Pipeline(steps, pipename, null);
        Map<String, Pipeline> namedPipeLine = Collections.singletonMap(pipename, pipe);
        EventsRepository<Future<?>> repository = new EventsRepository<Future<?>>(mainQueue, namedPipeLine);
        EventsProcessor ep = new EventsProcessor(mainQueue, outputQueues, namedPipeLine, 100, repository);
        sent.inject(pipe, mainQueue);
        Processor processor;
        ProcessingStatus ps = new ProcessingStatus();
        ps.mainQueue = mainQueue;
        ps.status = new ArrayList<>();
        while ((processor = sent.next()) != null) {
            int status = ep.process(sent, processor);
            ps.status.add(status);
        }
        return ps;
    }

}
