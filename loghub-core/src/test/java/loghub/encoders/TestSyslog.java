package loghub.encoders;

import java.beans.IntrospectionException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.RouteParser;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.senders.InMemorySender;

public class TestSyslog {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testParsing() {
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Syslog { }}}", RouteParser::output);
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Syslog {format: \"RFC5424\" }}}", RouteParser::output);
    }

    private Syslog.Builder getSampleBuilder() {
        Syslog.Builder builder = Syslog.getBuilder();
        builder.setCharset("UTF-8");
        builder.setFormat(Syslog.Format.RFC5424);
        builder.setVersion(2);
        builder.setDefaultTimeZone("UTC");
        // 5 and 20 are taken from the RFC and are said generate a priority of 165
        builder.setSeverity(new Expression(5));
        builder.setFacility(new Expression("20"));
        builder.setHostname(new Expression("HOSTNAME"));
        builder.setAppname(new Expression("APP-NAME"));
        builder.setProcid(new Expression("PROCID"));
        builder.setMsgid(new Expression("MSGID"));
        builder.setSecFrac(4);
        builder.setMessage(ConfigurationTools.unWrap("[message]", RouteParser::expression));
        return builder;
    }

    @Test
    public void testRfc3164() throws EncodeException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setFormat(Syslog.Format.RFC3164);
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "unit test");

        byte[] resultbytes = encoder.encode(e);
        String result = new String(resultbytes, StandardCharsets.US_ASCII);
        Assert.assertEquals("<165> Thu Jan 01 00:00:00.0000 1970 HOSTNAME unit test", result);
    }

    @Test
    public void testfull() throws EncodeException {
        Syslog encoder = getSampleBuilder().build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "unit test");

        byte[] resultbytes = encoder.encode(e);
        String result = new String(resultbytes, StandardCharsets.US_ASCII);
        Assert.assertEquals("<165>2 1970-01-01T00:00:00Z HOSTNAME APP-NAME PROCID MSGID [loghub] unit test", result);
    }

    @Test
    public void testbom() throws EncodeException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setWithbom(true);
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "éèœ");

        byte[] resultbytes = encoder.encode(e);
        String prefix = new String(Arrays.copyOf(resultbytes, 68), StandardCharsets.US_ASCII);
        Assert.assertEquals("<165>2 1970-01-01T00:00:00Z HOSTNAME APP-NAME PROCID MSGID [loghub] ", prefix);
        byte[] bom = Arrays.copyOfRange(resultbytes, 68, 71);
        Assert.assertArrayEquals(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, bom);
        String message = new String(Arrays.copyOfRange(resultbytes, 71, resultbytes.length), StandardCharsets.UTF_8);
        Assert.assertEquals("éèœ", message);
    }

    @Test
    public void testbadseverity() {
        Syslog.Builder builder = getSampleBuilder();
        builder.setSeverity(new Expression(8));
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        EncodeException ex = Assert.assertThrows(EncodeException.class, () -> encoder.encode(e));
        Assert.assertEquals("Invalid severity: 8", ex.getMessage());
    }

    @Test
    public void testbadfacility() {
        Syslog.Builder builder = getSampleBuilder();
        builder.setFacility(new Expression(24));
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        EncodeException ex = Assert.assertThrows(EncodeException.class, () -> encoder.encode(e));
        Assert.assertEquals("Invalid facility: 24", ex.getMessage());
    }

    @Test
    public void tesBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.encoders.Syslog"
                , BeanChecks.BeanInfo.build("format", Syslog.Format.class)
                , BeanChecks.BeanInfo.build("severity", Expression.class)
                , BeanChecks.BeanInfo.build("facility", Expression.class)
                , BeanChecks.BeanInfo.build("version", Integer.TYPE)
                , BeanChecks.BeanInfo.build("hostname", Expression.class)
                , BeanChecks.BeanInfo.build("appname", Expression.class)
                , BeanChecks.BeanInfo.build("procid", Expression.class)
                , BeanChecks.BeanInfo.build("msgid", Expression.class)
                , BeanChecks.BeanInfo.build("timestamp", Expression.class)
                , BeanChecks.BeanInfo.build("secFrac", Integer.TYPE)
                , BeanChecks.BeanInfo.build("message", Expression.class)
                , BeanChecks.BeanInfo.build("withbom", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("charset", String.class)
                , BeanChecks.BeanInfo.build("dateFormat", String.class)
        );
    }

}
