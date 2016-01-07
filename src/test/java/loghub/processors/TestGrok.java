package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.PipeStep;
import loghub.configuration.Properties;

public class TestGrok {




    @Test
    public void TestLoadPatterns() {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("%{COMBINEDAPACHELOG}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        PipeStep.EventWrapper e = new PipeStep.EventWrapper(new Event());
        e.processor = grok;
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        grok.process(e);

        Assert.assertEquals("Didn't find the good user agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22", e.get("agent"));
    }
}

