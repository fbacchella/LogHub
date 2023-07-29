package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
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
        Grok.Builder builder = Grok.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setPattern("%{COMBINEDAPACHELOG}");
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        e.process(grok);

        Assert.assertEquals("Didn't find the good user agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22", e.get("agent"));
    }

    @Test
    public void TestLoadPatterns2() throws ProcessorException {
        Grok.Builder builder = Grok.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setPattern("(?:%{SYSLOG_LINE})");
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "<34>1 2016-01-25T12:28:00.164593+01:00 somehost krb5kdc 4906 - -  closing down fd 14");
        e.process(grok);

        Assert.assertEquals("invalid syslog line matching", 8, e.size());
    }

    @Test
    public void TestLoadPatterns3() throws ProcessorException {
        Grok.Builder builder = Grok.getBuilder();
        builder.setCustomPatterns(Collections.singletonMap("FETCHING", "fetching user_deny.db entry"));
        builder.setPattern("%{FETCHING:message} for '%{USERNAME:imap_user}'");
        builder.setField(VariablePath.of("message"));
        Grok grok = builder.build();

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
        Grok.Builder builder = Grok.getBuilder();
        builder.setPattern("%{HOSTNAME:.}\\.google\\.com");
        Grok grok = builder.build();

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
        Grok.Builder builder = Grok.getBuilder();
        builder.setFields(new String[]{"remotehost"});
        builder.setPattern("%{HOSTNAME:.}\\.google\\.com");
        Grok grok = builder.build();

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
        Grok.Builder builder = Grok.getBuilder();
        builder.setFields(new String[]{"remotehost"});
        builder.setPattern("%{HOSTNAME:google.com}\\.google\\.com");
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("remotehost", "www.google.com");
        Tools.runProcessing(e, "main", Collections.singletonList(grok));
        Assert.assertEquals("invalid FQDN matching", "www", e.getAtPath(VariablePath.of("google", "com")));
    }

    @Test
    public void TestTyped() throws ProcessorException {
        Grok.Builder builder = Grok.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setPattern("%{INT:value:long}");
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("message", "1");
        e.process(grok);
        Assert.assertEquals("invalid line matching", 1L, e.get("value"));
    }

    @Test
    public void TestNoMatch() throws ProcessorException {
        Grok.Builder builder = Grok.getBuilder();
        builder.setFields(new String[]{"host"});
        builder.setPattern("%{HOSTNAME:.}\\.google\\.com");
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("host", "www.yahoo.com");
        Assert.assertEquals("FAILED", grok.fieldFunction(e, "www.yahoo.com").toString());
    }

    @Test
    public void TestMultiPattern() throws ProcessorException {
        Grok.Builder builder = Grok.getBuilder();
        builder.setFields(new String[]{"host"});
        builder.setPatterns(List.of("%{HOSTNAME:google}\\.google\\.com", "%{HOSTNAME:yahoo}\\.yahoo\\.com").toArray(String[]::new));
        Grok grok = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = factory.newEvent();
        e.put("host", "www.yahoo.com");
        Object status = grok.fieldFunction(e, "www.yahoo.com");
        Assert.assertEquals("NOSTORE", status.toString());
        Assert.assertEquals("www", e.get("yahoo"));
    }

    @Test
    public void TestBadPattern() {
        Grok.Builder builder = Grok.getBuilder();
        builder.setPattern("*");
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertTrue(ex.getMessage().startsWith("Unable to load patterns: Dangling meta character '*' near index 0"));
    }

    @Test
    public void test_loghub_processors_Grok() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Grok"
                , BeanChecks.BeanInfo.build("pattern", String.class)
                , BeanChecks.BeanInfo.build("customPatterns", Map.class)
                , BeanChecks.BeanInfo.build("classLoader", ClassLoader.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }
}
