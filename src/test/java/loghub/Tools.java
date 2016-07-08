package loghub;

import java.io.IOException;
import java.util.Locale;

import org.junit.Assert;

import loghub.configuration.Configuration;

public class Tools {

    static public void configure() throws IOException {
        Locale.setDefault(new Locale("POSIX"));
        System.getProperties().setProperty("java.awt.headless","true");
        System.setProperty("java.io.tmpdir",  "tmp");
        LogUtils.configure();
    }

    public static Configuration loadConf(String configname, boolean dostart) {
        String conffile = Configuration.class.getClassLoader().getResource(configname).getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }

        return conf;
    }
    
    public static Configuration loadConf(String configname) {
        return loadConf(configname, true);
    }

    public static Event getEvent() {
        return new EventInstance();
    }

}
