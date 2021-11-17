package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Event;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.Event.Action;
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
        sp.setField(VariablePath.of(new String[] {"message"}));

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "38");
        Assert.assertTrue(e.process(sp));
        String severity = (String)((Map<String, Object>)e.get("message")).get("severity");
        String facility = (String)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals("informational", severity);
        Assert.assertEquals("security/authorization", facility);
    }

    @Test
    public void TestResolvedStringEcs() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));
        sp.setEcs(true);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", "38");
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("informational", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "severity", "name"}), null));
        Assert.assertEquals(6, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "severity", "code"}), null));
        Assert.assertEquals("security/authorization", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "facility", "name"}), null));
        Assert.assertEquals(4, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "facility", "code"}), null));
        Assert.assertEquals(38, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "priority"}), null));
    }

    @Test
    public void TestResolvedStringEcsOverflow() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));
        sp.setEcs(true);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", 255);
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("debug", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "severity", "name"}), null));
        Assert.assertEquals(7, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "severity", "code"}), null));

        Assert.assertEquals(NullOrMissingValue.MISSING, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "facility", "name"}), null));
        Assert.assertEquals(31, e.applyAtPath(Action.GET, VariablePath.of(new String[] {"log", "syslog", "facility", "code"}), null));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedString() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));
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
        sp.setField(VariablePath.of(new String[] {"message"}));

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

    @Test
    public void TestResolvedStringOverflow() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));
        sp.setEcs(false);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", 255);
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("debug", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"message", "severity"}), null));
        Assert.assertEquals("31", e.applyAtPath(Action.GET, VariablePath.of(new String[] {"message", "facility"}), null));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedNumber() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));
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

    @Test(expected=ProcessorException.class)
    public void TestNotMatch() throws ProcessorException {
        SyslogPriority sp = new SyslogPriority();
        sp.setField(VariablePath.of(new String[] {"message"}));

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = Tools.getEvent();
        e.put("message", Instant.now());
        e.process(sp);
    }

    @Test
    public void test_loghub_processors_SyslogPriority() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.SyslogPriority"
                              , BeanChecks.BeanInfo.build("Facilities", BeanChecks.LSTRING)
                              , BeanChecks.BeanInfo.build("Severities", BeanChecks.LSTRING)
                              , BeanChecks.BeanInfo.build("ecs", Boolean.TYPE)
                              , BeanChecks.BeanInfo.build("resolve", Boolean.TYPE)
                        );
    }

}
