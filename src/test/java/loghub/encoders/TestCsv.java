package loghub.encoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

public class TestCsv {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, Csv.class.getName());
    }

    private Expression parseExpression(String exp) {
        return ConfigurationTools.unWrap(exp, RouteParser::expression, Collections.emptyMap());
    }

    private void check(Csv.Builder builder, String expectedFormat) throws EncodeException {
        Expression[] columns = Stream.of("[K1]", "[K2]", "[K3]", "[K4]", "[K5]", "[K6]").map(this::parseExpression).toArray(Expression[]::new);
        builder.setValues(columns);
        Csv encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        e.put("K1", "V1");
        e.put("K2", 2);
        e.put("K3", true);
        e.put("K4", Instant.EPOCH);
        e.put("K5", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")));
        e.put("K6", new Date(0));

        byte[] result = encoder.encode(e);

        String formatted = new String(result, StandardCharsets.UTF_8);
        Assert.assertEquals(expectedFormat, formatted);
    }

    @Test
    public void testDefault() throws EncodeException {
        Csv.Builder builder = Csv.getBuilder();
        builder.setZoneId("UTC");
        check(builder, "\"V1\",2,true,\"1970-01-01T00:00:00.000Z\",\"1970-01-01T00:00:00.000Z\",\"1970-01-01T00:00:00.000Z\"\n");
    }

    @Test
    public void testArray() throws EncodeException {
        Expression[] columns = Stream.of("[a]", "[b]").map(this::parseExpression).toArray(Expression[]::new);
        Csv.Builder builder = Csv.getBuilder();
        builder.setZoneId("UTC");
        builder.setValues(columns);
        Csv encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e1 = factory.newEvent();
        e1.put("a", 1);
        e1.put("b", 2);
        Event e2 = factory.newEvent();
        e2.put("a", 3);
        e2.put("b", 4);
        byte[] result = encoder.encode(Stream.of(e1, e2));

        String formatted = new String(result, StandardCharsets.UTF_8);
        Assert.assertEquals("1,2\n3,4\n", formatted);
    }

    @Ignore
    @Test
    public void testParse() throws EncodeException {
        String confFragment = "loghub.encoders.Csv {values: [0; true; [a]; [b]], separator: '|', features: []}";
        Csv encoder = ConfigurationTools.unWrap(confFragment, RouteParser::object);
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e1 = factory.newEvent();
        e1.put("a", 1);
        e1.put("b", "2");
        byte[] result = encoder.encode(e1);
        String formatted = new String(result, StandardCharsets.UTF_8);
        Assert.assertEquals("0|true|1|\"2\"\n", formatted);
    }

    @Test
    public void testBad() throws EncodeException {
        Expression[] columns = Stream.of("[a]", "1/[b]").map(this::parseExpression).toArray(Expression[]::new);
        Csv.Builder builder = Csv.getBuilder();
        builder.setZoneId("UTC");
        builder.setValues(columns);
        Csv encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e1 = factory.newEvent();
        e1.put("c", 1);
        Event e2 = factory.newEvent();
        e2.put("b", 0);
        byte[] result = encoder.encode(Stream.of(e1, e2));

        String formatted = new String(result, StandardCharsets.UTF_8);
        Assert.assertEquals("\"Missing value\",\"Missing value\"\n\"Missing value\",\"Division by zero\"\n", formatted);
    }

    @Test
    public void testSemicolon() throws EncodeException {
        Csv.Builder builder = Csv.getBuilder();
        builder.setZoneId("UTC");
        builder.setSeparator(';');
        check(builder, "\"V1\";2;true;\"1970-01-01T00:00:00.000Z\";\"1970-01-01T00:00:00.000Z\";\"1970-01-01T00:00:00.000Z\"\n");
    }

    @Test
    public void testWindows() throws EncodeException {
        Csv.Builder builder = Csv.getBuilder();
        builder.setZoneId("Europe/Paris");
        builder.setSeparator(';');
        builder.setLineSeparator("\r\n");
        check(builder, "\"V1\";2;true;\"1970-01-01T01:00:00.000+01:00\";\"1970-01-01T00:00:00.000Z\";\"1970-01-01T01:00:00.000+01:00\"\r\n");
    }

    @Test
    public void test_loghub_encoders_Csv() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.encoders.Csv"
                , BeanChecks.BeanInfo.build("values", Object[].class)
                , BeanChecks.BeanInfo.build("features", Object[].class)
                , BeanChecks.BeanInfo.build("separator", Character.TYPE)
                , BeanChecks.BeanInfo.build("lineSeparator", String.class)
                , BeanChecks.BeanInfo.build("nullValue", String.class)
                , BeanChecks.BeanInfo.build("charset", String.class)
                , BeanChecks.BeanInfo.build("zoneId", String.class)
                , BeanChecks.BeanInfo.build("locale", String.class)
        );
    }

}
