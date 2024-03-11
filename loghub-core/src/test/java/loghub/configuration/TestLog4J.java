package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;

public class TestLog4J {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration");
    }

    @Test
    public void testlog4j() throws ConfigException, IOException {
        Map<String, Object> props = new HashMap<>();
        URL xmlfile = getClass().getClassLoader().getResource("testlog4j.xml");
        logger.debug("log4j2 configuration file is {}", xmlfile);
        String configFile = String.format("log4j.configURL: \"%s\"", xmlfile);
        props.put("log4j.configURL", xmlfile.toString());
        @SuppressWarnings("unused")
        Properties lhprops =  Tools.loadConf(new StringReader(configFile));
        LoggerContext ctx = (LoggerContext) LogManager.getContext(true);
        org.apache.logging.log4j.core.config.Configuration conf = ctx.getConfiguration();
        Assert.assertNotNull("TestConsole appender not found", conf.getAppender("TestConsole"));
    }

}
