package loghub.encoders;

import java.beans.IntrospectionException;
import java.io.IOException;
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
import loghub.Event;
import loghub.Expression;
import loghub.LogUtils;
import loghub.RouteParser;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.senders.InMemorySender;

public class TestSyslog {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testParsing() {
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Syslog { }}}", i -> i.output());
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Syslog {format: \"rfc5424\" }}}", i -> i.output());
    }

    private Syslog.Builder getSampleBuilder() {
        Syslog.Builder builder = Syslog.getBuilder();
        builder.setCharset("UTF-8");
        builder.setFormat("rFc5424");
        builder.setVersion(2);
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
    public void testRfc3164() throws EncodeException, Expression.ExpressionException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setFormat("rFc3164");
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "unit test");

        byte[] resultbytes = encoder.encode(e);
        String result = new String(resultbytes, StandardCharsets.US_ASCII);
        Assert.assertEquals("<165> Thu Jan 01 00:00:00.0000 1970 HOSTNAME unit test", result);
    }

    @Test
    public void testfull() throws EncodeException, Expression.ExpressionException {
        Syslog encoder = getSampleBuilder().build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "unit test");

        byte[] resultbytes = encoder.encode(e);
        String result = new String(resultbytes, StandardCharsets.US_ASCII);
        Assert.assertEquals("<165>2 1970-01-01T00:00:00.0000Z HOSTNAME APP-NAME PROCID MSGID [loghub] unit test", result);
    }

    @Test
    public void testbom() throws EncodeException, Expression.ExpressionException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setWithbom(true);
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        e.setTimestamp(new Date(0));
        e.put("message", "éèœ");

        byte[] resultbytes = encoder.encode(e);
        String prefix = new String(Arrays.copyOf(resultbytes, 73), StandardCharsets.US_ASCII);
        Assert.assertEquals("<165>2 1970-01-01T00:00:00.0000Z HOSTNAME APP-NAME PROCID MSGID [loghub] ", prefix);
        byte[] bom = Arrays.copyOfRange(resultbytes, 73, 76);
        Assert.assertArrayEquals(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, bom);
        String message = new String(Arrays.copyOfRange(resultbytes, 76, resultbytes.length), StandardCharsets.UTF_8);
        Assert.assertEquals("éèœ", message);
    }

    @Test
    public void testbadseverity() throws EncodeException, Expression.ExpressionException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setSeverity(new Expression(8));
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        EncodeException ex = Assert.assertThrows(EncodeException.class, () -> encoder.encode(e));
        Assert.assertEquals("Invalid severity: 8", ex.getMessage());
    }

    @Test
    public void testbadfacility() throws EncodeException, Expression.ExpressionException {
        Syslog.Builder builder = getSampleBuilder();
        builder.setFacility(new Expression(24));
        Syslog encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        EncodeException ex = Assert.assertThrows(EncodeException.class, () -> encoder.encode(e));
        Assert.assertEquals("Invalid facility: 24", ex.getMessage());
    }

    @Test
    public void test_loghub_processors_Syslog() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.encoders.Syslog"
                , BeanChecks.BeanInfo.build("format", String.class)
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
