package loghub.processors;

import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

import loghub.Event;
import loghub.configuration.Properties;

public class TestUserAgent {

    @Test
    public void test() {
        UserAgent ua = new UserAgent();
        ua.setField("User-Agent");
        ua.setCacheSize(10);
        ua.configure(new Properties(Collections.emptyMap()));
        
        String uaString = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";
        
        Event event = new Event();
        event.put("User-Agent", uaString);
        ua.process(event);
        System.out.println(event);
    }

}
