package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestOnigurumaRegex {

    @Test
    public void TestLoadPatterns1() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField("message");
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<15>a text");
        e.process(grok);

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "a text", e.get("message"));
    }

    @Test
    public void TestLoadPatterns2() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField("message");
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<char>.)(?<char>.)(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertFalse("Failed to configure grok", grok.configure(props));
    }

    // Test missing optionnal pattern
    @Test
    public void TestLoadPatterns4() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField("message");
        grok.setPattern("^(?<prefix>\\*|\\.)?(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "a text");
        e.process(grok);
        Assert.assertEquals("Didn't find the good message", "a text", e.get("message"));
        Assert.assertEquals("Should not have found the prefix", null, e.get("prefix"));
    }

}
