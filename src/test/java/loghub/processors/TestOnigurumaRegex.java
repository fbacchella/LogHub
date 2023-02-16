package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestOnigurumaRegex {

    private final EventsFactory factory = new EventsFactory();

    @Test
    public void testLoadPatterns1() throws ProcessorException {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("<(?<syslog_pri>\\d+)>(?<message>.*)");
        OnigurumaRegex grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<15>a text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "a text", e.get("message"));
    }

    @Test
    public void testLoadPatterns2() {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("<(?<syslog_pri>\\d+)>(?<char>.)(?<char>.)(?<message>.*)");
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertEquals("Can't have two captures with same name", ex.getMessage());
    }

    // Test missing optionnal pattern
    @Test
    public void testLoadPatterns3() throws ProcessorException {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("^(?<prefix>\\*|\\.)?(?<message>.*)");
        OnigurumaRegex grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "a text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good message", "a text", e.get("message"));
        Assert.assertNull("Should not have found the prefix", e.get("prefix"));
    }

    @Test
    public void testUtf1() throws ProcessorException {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("<(?<syslog_pri>\\d+)>(?<message>.*)");
        OnigurumaRegex grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<15>a textà");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "a textà", e.get("message"));
    }

    @Test
    public void testUtf2() throws ProcessorException {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("<(?<syslog_pri>\\d+)>(?<message>é.*)");
        OnigurumaRegex grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<15>éa text");
        Assert.assertTrue(e.process(grok));

        Assert.assertEquals("Didn't find the good syslog priority", "15", e.get("syslog_pri"));
        Assert.assertEquals("Didn't find the good syslog message", "éa text", e.get("message"));
    }

    @Test
    public void testNoNamedPattern() throws ProcessorException {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern(".*");
        OnigurumaRegex grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<15>a text");
        Assert.assertTrue(e.process(grok));
    }

    @Test
    public void testBadPattern() {
        OnigurumaRegex.Builder builder = OnigurumaRegex.getBuilder();
        builder.setField(VariablePath.of(new String[] {"message"}));
        builder.setPattern("*");
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertEquals("Error parsing regex '*': target of repeat operator is not specified", ex.getMessage());
    }

}
