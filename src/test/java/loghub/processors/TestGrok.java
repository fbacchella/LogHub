package loghub.processors;

import java.io.IOException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.events.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.events.Event.Action;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestGrok {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Grok", "loghub.EventsProcessor");
    }

    @Test
    public void TestLoadPatterns1() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("%{COMBINEDAPACHELOG}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        e.process(grok);

        Assert.assertEquals("Didn't find the good user agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22", e.get("agent"));
    }

    @Test
    public void TestLoadPatterns2() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("(?:%{SYSLOG_LINE})");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<34>1 2016-01-25T12:28:00.164593+01:00 somehost krb5kdc 4906 - -  closing down fd 14");
        e.process(grok);

        Assert.assertEquals("invalid syslog line matching", 8, e.size());
    }

    @Test
    public void TestLoadPatterns3() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setCustomPatterns(Collections.singletonMap("FETCHING", "fetching user_deny.db entry"));
        grok.setPattern("%{FETCHING:message} for '%{USERNAME:imap_user}'");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "fetching user_deny.db entry for 'someone'");
        e.process(grok);
        Assert.assertEquals("invalid syslog line matching", 2, e.size());
        Assert.assertEquals("invalid syslog line matching", "someone", e.get("imap_user"));
    }

    @Test
    public void TestLoadPatterns5() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"localhost"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("localhost", "127.0.0.1");
        e.put("remotehost", "www.google.com");
        e.put("remotehostother", "www.google.com");
        e.put("host", "www.yahoo.com");
        Tools.runProcessing(e, "main", Collections.singletonList(grok));
        Assert.assertEquals("invalid FQDN matching", "www.google.com", e.get("remotehost"));
        Assert.assertEquals("invalid FQDN matching", "127.0.0.1", e.get("localhost"));
    }

    @Test
    public void TestLoadPatterns6() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"remotehost"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("localhost", "127.0.0.1");
        e.put("remotehost", "www.google.com");
        e.put("remotehostother", "www.google.com");
        e.put("host", "www.yahoo.com");
        Tools.runProcessing(e, "main", Collections.singletonList(grok));
        Assert.assertEquals("invalid FQDN matching", "www", e.get("remotehost"));
    }

    @Test
    public void TestWithPath() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"remotehost"});
        grok.setPattern("%{HOSTNAME:google.com}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("remotehost", "www.google.com");
        Tools.runProcessing(e, "main", Collections.singletonList(grok));
        Assert.assertEquals("invalid FQDN matching", "www", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"google", "com"}) , null));
    }

    @Test
    public void TestTyped() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField(VariablePath.of(new String[] {"message"}));
        grok.setPattern("%{INT:value:long}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "1");
        e.process(grok);
        Assert.assertEquals("invalid line matching", 1L, e.get("value"));
    }

    @Test
    public void TestNoMatch() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"host"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("host", "www.yahoo.com");
        Assert.assertEquals("FAILED", grok.fieldFunction(e, "www.yahoo.com").toString());
    }

    @Test
    public void TestBadPattern() throws ProcessorException {
        Grok grok = new Grok();
        grok.setPattern("*");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertFalse("Failed to handle bad pattern", grok.configure(props));
    }

}
