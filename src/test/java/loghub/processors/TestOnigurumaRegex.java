package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;

public class TestOnigurumaRegex {

    @Test
    public void testLoadPatterns1() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<15>a text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "a text", e.get("message"));
    }

    @Test
    public void testLoadPatterns2() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<char>.)(?<char>.)(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertFalse("Failed to configure grok", grok.configure(props));
    }

    // Test missing optionnal pattern
    @Test
    public void testLoadPatterns3() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("^(?<prefix>\\*|\\.)?(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "a text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good message", "a text", e.get("message"));
        Assert.assertEquals("Should not have found the prefix", null, e.get("prefix"));
    }

    @Test
    public void testUtf1() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<message>.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<15>a textà");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "a textà", e.get("message"));
    }

    @Test
    public void testUtf2() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("<(?<syslog_pri>\\d+)>(?<message>é.*)");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<15>éa text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "éa text", e.get("message"));
    }

    @Test
    public void testNoNamedPattern() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern(".*");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<15>a text");
        Assert.assertTrue(e.process(grok));
    }

    @Test
    public void testBadPattern() throws ProcessorException {
        OnigurumaRegex grok = new OnigurumaRegex();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("*");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertFalse("Failed to handle bad pattern", grok.configure(props));
    }

}
