package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.time.Instant;
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
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestSyslogPriority {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.SyslogPriority");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestResolvedString() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", "86");
        Assert.assertTrue(e.process(sp));
        String severity = (String)((Map<String, Object>)e.get("message")).get("severity");
        String facility = (String)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals("informational", severity);
        Assert.assertEquals("security/authorization", facility);
    }

    @Test
    public void TestResolvedStringInPlaceEcs() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setInPlace(true);
        builder.setEcs(true);
        builder.setField(VariablePath.of("priority"));
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        VariablePath syslogPath = VariablePath.of(List.of("log", "syslog"));
        e.putAtPath(syslogPath.append("priority"), 38);
        e = e.wrap(syslogPath);
        Assert.assertTrue(e.process(sp));
        e = e.unwrap();
        @SuppressWarnings("unchecked")
        Map<String, Object> severity = (Map<String, Object>) e.getAtPath(syslogPath.append("severity"));
        @SuppressWarnings("unchecked")
        Map<String, Object> facility = (Map<String, Object>) e.getAtPath(syslogPath.append("facility"));
        Assert.assertEquals(Map.of("name", "informational", "code", 6), severity);
        Assert.assertEquals(Map.of("name", "security/authorization", "code", 4), facility);
    }

    @Test
    public void TestResolvedStringEcs() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setEcs(true);
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", "38");
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("informational", e.getAtPath(VariablePath.of("log", "syslog", "severity", "name")));
        Assert.assertEquals(6, e.getAtPath(VariablePath.of("log", "syslog", "severity", "code")));
        Assert.assertEquals("security/authorization", e.getAtPath(VariablePath.of("log", "syslog", "facility", "name")));
        Assert.assertEquals(4, e.getAtPath(VariablePath.of("log", "syslog", "facility", "code")));
        Assert.assertEquals(38, e.getAtPath(VariablePath.of("log", "syslog", "priority")));
    }

    @Test
    public void TestResolvedStringEcsOverflow() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setEcs(true);
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", 255);
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("debug", e.getAtPath(VariablePath.of("log", "syslog", "severity", "name")));
        Assert.assertEquals(7, e.getAtPath(VariablePath.of("log", "syslog", "severity", "code")));

        Assert.assertEquals(NullOrMissingValue.MISSING, e.getAtPath(VariablePath.of("log", "syslog", "facility", "name")));
        Assert.assertEquals(31, e.getAtPath(VariablePath.of("log", "syslog", "facility", "code")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedString() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setResolve(false);
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
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
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", 38);
        e.process(sp);
        String severity = (String)((Map<String, Object>)e.get("message")).get("severity");
        String facility = (String)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals("informational", severity);
        Assert.assertEquals("security/authorization", facility);
    }

    @Test
    public void TestResolvedStringOverflow() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setEcs(false);
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", 255);
        Assert.assertTrue(e.process(sp));
        Assert.assertEquals("debug", e.getAtPath(VariablePath.of("message", "severity")));
        Assert.assertEquals("31", e.getAtPath(VariablePath.of("message", "facility")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNotResolvedNumber() throws ProcessorException {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setResolve(false);
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", 38);
        e.process(sp);
        Number severity = (Number)((Map<String, Object>)e.get("message")).get("severity");
        Number facility = (Number)((Map<String, Object>)e.get("message")).get("facility");
        Assert.assertEquals(6, severity);
        Assert.assertEquals(4, facility);
    }

    @Test
    public void TestNotMatch() {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setField(VariablePath.of("message"));
        SyslogPriority sp = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(sp.configure(props));

        Event e = factory.newEvent();
        e.put("message", Instant.now());
        Assert.assertThrows(ProcessorException.class, () -> e.process(sp));
    }

    @Test
    public void test_loghub_processors_SyslogPriority() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.SyslogPriority"
                              , BeanChecks.BeanInfo.build("facilities", BeanChecks.LSTRING)
                              , BeanChecks.BeanInfo.build("severities", BeanChecks.LSTRING)
                              , BeanChecks.BeanInfo.build("ecs", Boolean.TYPE)
                              , BeanChecks.BeanInfo.build("resolve", Boolean.TYPE)
                              , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
                        );
    }

}
