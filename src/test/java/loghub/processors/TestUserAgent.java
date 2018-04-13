package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestUserAgent {

    @Test
    public void test1() throws ProcessorException {
        UserAgent ua = new UserAgent();
        ua.setField("User-Agent");
        ua.setCacheSize(10);
        ua.setDestination("agent");
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));

        String uaString = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";

        Event event = Tools.getEvent();
        event.put("User-Agent", uaString);
        Assert.assertTrue(ua.process(event));
        Object family = event.applyAtPath((i, j, k) -> i.get(j), new String[] {"agent", "userAgent", "family"}, null, false);
        Assert.assertEquals("can't find user agent parsing", "Mobile Safari", family);
    }
    
    @Test
    public void testDownload() throws ProcessorException {
        UserAgent ua = new UserAgent();
        ua.setField("User-Agent");
        ua.setCacheSize(10);
        ua.setDestination("agent");
        ua.setAgentsUrl("https://raw.githubusercontent.com/ua-parser/uap-core/master/regexes.yaml");
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));
    }

}
