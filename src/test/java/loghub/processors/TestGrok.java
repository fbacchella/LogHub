package loghub.processors;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestGrok {

    @Test
    public void TestLoadPatterns() throws ProcessorException {
        Grok grok = new Grok();
        grok.setField("message");
        grok.setPattern("%{COMBINEDAPACHELOG}");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        e.process(grok);

        Assert.assertEquals("Didn't find the good user agent", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"", e.get("agent"));
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
        grok.setPattern("(?<message>fetching user_deny.db entry) for '%{USERNAME:imap_user}'");

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
        grok.setFields(new String[] {"localhost", "remotehost"});
        grok.setPattern("%{HOSTNAME:.}\\.google\\.com");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue("Failed to configure grok", grok.configure(props));

        Event e = Tools.getEvent();
        e.put("localhost", "127.0.0.1");
        e.put("remotehost", "www.google.com");
        e.process(grok);
        Assert.assertEquals("invalid FQDN matching", "www", e.get("remotehost"));
        Assert.assertEquals("invalid FQDN matching", "127.0.0.1", e.get("localhost"));
    }

}
