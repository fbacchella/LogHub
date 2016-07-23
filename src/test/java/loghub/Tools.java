package loghub;

import java.io.IOException;
import java.util.Locale;

import org.junit.Assert;

import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class Tools {

    static public void configure() throws IOException {
        Locale.setDefault(new Locale("POSIX"));
        System.getProperties().setProperty("java.awt.headless","true");
        System.setProperty("java.io.tmpdir", "tmp");
        LogUtils.configure();
    }

    public static Properties loadConf(String configname, boolean dostart) {
        String conffile = Configuration.class.getClassLoader().getResource(configname).getFile();
        Properties props = Configuration.parse(conffile);

        for(Pipeline pipe: props.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(props));
        }

        return props;
    }

    public static Properties loadConf(String configname) {
        return loadConf(configname, true);
    }

    public static Event getEvent() {
        return new EventInstance();
    }

    public static void runProcessing(Event sent, Pipeline pipe, Properties props) throws ProcessorException {
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps);
        sent.inject(pipe, props.mainQueue);
        Processor processor;
        while ((processor = sent.next()) != null) {
            ep.process(sent, processor);
        }
    }

}
