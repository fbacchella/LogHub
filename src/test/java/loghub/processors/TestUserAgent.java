package loghub.processors;

import java.io.IOException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.Event.Action;
import loghub.configuration.Properties;

public class TestUserAgent {
    
    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.UserAgent");
    }

    @Test
    public void test1() throws ProcessorException {
        UserAgent ua = new UserAgent();
        ua.setField(VariablePath.of(new String[] {"User-Agent"}));
        ua.setCacheSize(10);
        ua.setDestination(VariablePath.of("agent"));
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));

        String uaString = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";

        Event event = Tools.getEvent();
        event.put("User-Agent", uaString);
        Assert.assertTrue(ua.process(event));
        Object family = event.applyAtPath(Action.GET, VariablePath.of(new String[] {"agent", "userAgent", "family"}), null, false);
        Assert.assertEquals("can't find user agent parsing", "Mobile Safari", family);
    }

    @Test
    public void testDownload() throws ProcessorException {
        UserAgent ua = new UserAgent();
        ua.setField(VariablePath.of(new String[] {"User-Agent"}));
        ua.setCacheSize(10);
        ua.setDestination(VariablePath.of("agent"));
        ua.setAgentsUrl(TestUserAgent.class.getClassLoader().getResource("regexes.yaml").toString());
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));
    }

}
