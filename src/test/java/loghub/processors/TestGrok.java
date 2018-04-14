package loghub.processors;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestGrok {

    @Test
    public void TestLoadPatterns1() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("%{COMBINEDAPACHELOG}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        e.process(grok);

        Assert.assertEquals("Didn't find the good user agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22", e.get("agent"));
    }

    @Test
    public void TestLoadPatterns2() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("(?:%{SYSLOG_LINE})");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "<34>1 2016-01-25T12:28:00.164593+01:00 somehost krb5kdc 4906 - -  closing down fd 14");
        e.process(grok);

        Assert.assertEquals("invalid syslog line matching", 8, e.size());
    }

    @Test
    public void TestLoadPatterns3() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setCustomPatterns(Collections.singletonMap("FETCHING", "fetching user_deny.db entry"));
        grok.setPattern("%{FETCHING:message} for '%{USERNAME:imap_user}'");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "fetching user_deny.db entry for 'someone'");
        e.process(grok);

        Assert.assertEquals("invalid syslog line matching", 2, e.size());
    }

    @Test
    public void TestLoadPatterns5() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"localhost"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
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

        Event e = Tools.getEvent();
        e.put("localhost", "127.0.0.1");
        e.put("remotehost", "www.google.com");
        e.put("remotehostother", "www.google.com");
        e.put("host", "www.yahoo.com");
        Tools.runProcessing(e, "main", Collections.singletonList(grok));
        Assert.assertEquals("invalid FQDN matching", "www", e.get("remotehost"));
    }

    @Test
    public void TestNoMatch() throws ProcessorException {
        Grok grok = new Grok();
        grok.setFields(new String[]{"host"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("host", "www.yahoo.com");
        Assert.assertFalse(grok.processMessage(e, "host", "host"));
    }

    // Will fails when issue https://github.com/thekrakken/java-grok/issues/64 is corrected
    @Test
    public void TestLoadPatterns8() {
        String pattern = "(?<message>client id): (?<clientid>.*)";
        String input = "client id: \"name\" \"Mac OS X Mail\" \"version\" \"10.2 (3259)\" \"os\" \"Mac OS X\" \"os-version\" \"10.12.3 (16D32)\" \"vendor\" \"Apple Inc.\"";

        // Validate the search is good
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(input);
        if (m.matches()) {
            Assert.assertEquals("\"name\" \"Mac OS X Mail\" \"version\" \"10.2 (3259)\" \"os\" \"Mac OS X\" \"os-version\" \"10.12.3 (16D32)\" \"vendor\" \"Apple Inc.\"", m.group("clientid"));
        }

        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();

        io.krakens.grok.api.Grok grok = grokCompiler.compile(pattern, true);

        Match gm = grok.match(input);
        Map<String, Object> captures = gm.capture();
        Assert.assertNotEquals(captures.get("clientid"), gm.getMatch().group("clientid"));
    }

}
