package loghub.processors;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestSyslogPriority {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.SyslogPriority");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestResolvedString() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField("message");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "38");
        e.process(sp);
        String severity = (String)((Map<String, Object>)e.get("message")).get("severity");
        String facility = (String)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals("informational", severity);
        Assert.assertEquals("security/authorization", facility);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedString() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField("message");
        sp.setResolve(false);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "38");
        e.process(sp);
        Number severity = (Number)((Map<String, Object>)e.get("message")).get("severity");
        Number facility = (Number)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals(6, severity);
        Assert.assertEquals(4, facility);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestResolvedNumber() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField("message");

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", 38);
        e.process(sp);
        String severity = (String)((Map<String, Object>)e.get("message")).get("severity");
        String facility = (String)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals("informational", severity);
        Assert.assertEquals("security/authorization", facility);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedNumber() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField("message");
        sp.setResolve(false);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", 38);
        e.process(sp);
        Number severity = (Number)((Map<String, Object>)e.get("message")).get("severity");
        Number facility = (Number)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals(6, severity);
        Assert.assertEquals(4, facility);
    }

}
