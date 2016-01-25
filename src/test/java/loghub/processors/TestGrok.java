package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.EventWrapper;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class TestGrok {

    @Test
    public void TestLoadPatterns() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("%{COMBINEDAPACHELOG}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(grok);
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        grok.process(e);

        Assert.assertEquals("Didn't find the good user agent", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"", e.get("agent"));
    }

    @Test
    public void TestLoadPatterns2() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("(?:%{SYSLOG_LINE})");
        
        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(grok);
        e.put("message", "<34>1 2016-01-25T12:28:00.164593+01:00 sys403 krb5kdc 4906 - -  closing down fd 14");
        grok.process(e);

        Assert.assertEquals("invalid syslog line matching", 8, e.size());
    }
}
