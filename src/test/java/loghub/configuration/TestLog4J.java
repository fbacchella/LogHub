package loghub.configuration;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.CountingNoOpAppender;
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
    public void testlog4j() {
        Map<String, Object> props = new HashMap<>();
        URL xmlfile = getClass().getClassLoader().getResource("testlog4j.xml");
        System.out.println(xmlfile);
        props.put("log4j.configURL", xmlfile.toString());
        @SuppressWarnings("unused")
        Properties lhprops = new Properties(props);
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration conf = ctx.getConfiguration();
        Assert.assertNotNull("TestConsole appender not found", conf.getAppender("TestConsole"));
        CountingNoOpAppender counting = conf.getAppender("counting");
        Assert.assertTrue("not enough logs", counting.getCount() >= 1);
    }

}
