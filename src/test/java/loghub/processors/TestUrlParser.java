package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

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

public class TestUrlParser {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testParsingGoodRelative() throws ProcessorException {
        UrlParser.Builder builder = UrlParser.getBuilder();
        builder.setReference("http://loghub.fr/");
        builder.setField(VariablePath.of(List.of("message")));
        builder.setInPlace(true);
        UrlParser parser = builder.build();
        Assert.assertTrue(parser.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message", "index.html");
        parser.process(event);
        Assert.assertEquals("/index.html", event.get("path"));
        Assert.assertEquals("http", event.get("scheme"));
        Assert.assertEquals("loghub.fr", event.get("domain"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParsingGoodAbsoluteDestination() throws ProcessorException {
        testGood(b -> b.setDestination(VariablePath.of(List.of("url"))), e -> (Map<String, Object>) e.get("url"));
    }

    @Test
    public void testParsingGoodAbsoluteInPlace() throws ProcessorException {
        testGood(b -> b.setInPlace(true), e -> e);
    }

    private void testGood(Consumer<UrlParser.Builder> configurator, Function<Event, Map<String, Object>> extractor) throws ProcessorException {
        UrlParser.Builder builder = UrlParser.getBuilder();
        builder.setField(VariablePath.of(List.of("message")));
        configurator.accept(builder);
        UrlParser parser = builder.build();
        Assert.assertTrue(parser.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message", "http://login:pass@loghub.fr:80/index.html?arg#ref");
        parser.process(event);
        checkUrlResolution(extractor.apply(event));
    }

    private void checkUrlResolution(Map<String, Object> values) {
        Assert.assertEquals("/index.html", values.get("path"));
        Assert.assertEquals("http", values.get("scheme"));
        Assert.assertEquals("loghub.fr", values.get("domain"));
        Assert.assertEquals("ref", values.get("fragment"));
        Assert.assertEquals(80, values.get("port"));
        Assert.assertEquals("arg", values.get("query"));
        Assert.assertEquals("login", values.get("username"));
        Assert.assertEquals("pass", values.get("password"));
    }

    @Test
    public void testParsingFailed() {
        UrlParser.Builder builder = UrlParser.getBuilder();
        builder.setField(VariablePath.of(List.of("message")));
        UrlParser parser = builder.build();
        Assert.assertTrue(parser.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message", "_http://loghub.fr:80/index.html");
        Assert.assertThrows(ProcessorException.class, () -> parser.process(event));
    }

    private void testContent(Map<String, Object> uriParts) {
        Assert.assertEquals("/foo.gif", uriParts.get("path"));
        Assert.assertEquals("frag", uriParts.get("fragment"));
        Assert.assertEquals("gif", uriParts.get("extension"));
        Assert.assertEquals("http", uriParts.get("scheme"));
        Assert.assertEquals(80, uriParts.get("port"));
        Assert.assertEquals("myusername:mypassword", uriParts.get("user_info"));
        Assert.assertEquals("www.example.com", uriParts.get("domain"));
        Assert.assertEquals("key1=val1&key2=val2", uriParts.get("query"));
        Assert.assertEquals("myusername", uriParts.get("username"));
        Assert.assertEquals("mypassword", uriParts.get("password"));
    }

    private Event runTest(Consumer<UrlParser.Builder> configure, String field, String uriString) throws ProcessorException {
        UrlParser.Builder builder = UrlParser.getBuilder();
        configure.accept(builder);
        UrlParser parts = builder.build();
        Assert.assertTrue(parts.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put(field, uriString);
        Assert.assertTrue(parts.process(event));
        return event;
    }

    @Test
    public void test1() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"input_field"}));
                    b.setDestination(VariablePath.of("url"));
                }, "input_field",
                "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        @SuppressWarnings("unchecked")
        Map<String, Object> uriParts = (Map<String, Object>) event.get("url");
        testContent(uriParts);
    }

    @Test
    public void test2() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        testContent(event);
    }

    @Test
    public void testExtention1() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/foo?key1=val1&key2=val2#frag");
        Assert.assertEquals("/foo", event.get("path"));
        Assert.assertFalse(event.containsKey("extension"));
    }

    @Test
    public void testExtention2() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/foo.?key1=val1&key2=val2#frag");
        Assert.assertEquals("/foo.", event.get("path"));
        Assert.assertFalse(event.containsKey("extension"));
    }

    @Test
    public void testExtention3() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/.gif?key1=val1&key2=val2#frag");
        Assert.assertEquals("/.gif", event.get("path"));
        Assert.assertEquals("gif", event.get("extension"));
    }

    @Test
    public void testUserInfo1() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        Assert.assertEquals("myusername", event.get("user_info"));
        Assert.assertFalse(event.containsKey("password"));
        Assert.assertFalse(event.containsKey("username"));
    }

    @Test
    public void testUserInfo2() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        Assert.assertEquals("myusername", event.get("username"));
        Assert.assertEquals("", event.get("password"));
    }

    @Test
    public void testUserInfo3() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://:password@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        Assert.assertEquals("", event.get("username"));
        Assert.assertEquals("password", event.get("password"));
    }

    @Test
    public void testRfcExample1() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "ftp://ftp.is.co.za/rfc/rfc1808.txt");
        System.err.println(event);
        Assert.assertEquals("ftp", event.get("scheme"));
    }

    @Test
    public void testRfcExample3() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "ldap://[2001:db8::7]/c=GB?objectClass?one");
        System.err.println(event);
        Assert.assertEquals("ldap", event.get("scheme"));
        Assert.assertEquals("[2001:db8::7]", event.get("domain"));
        Assert.assertEquals("objectClass?one", event.get("query"));
        Assert.assertEquals("/c=GB", event.get("path"));
    }

    @Test
    public void testRfcExample4() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "mailto:John.Doe@example.com");
        System.err.println(event);
        Assert.assertEquals("mailto", event.get("scheme"));
        Assert.assertEquals("John.Doe@example.com", event.get("specific"));
    }

    @Test
    public void testRfcExample5() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "news:comp.infosystems.www.servers.unix");
        System.err.println(event);
        Assert.assertEquals("news", event.get("scheme"));
        Assert.assertEquals("comp.infosystems.www.servers.unix", event.get("specific"));
    }

    @Test
    public void testRfcExample6() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "tel:+1-816-555-1212");
        System.err.println(event);
        Assert.assertEquals("tel", event.get("scheme"));
        Assert.assertEquals("+1-816-555-1212", event.get("specific"));
    }

    @Test
    public void testRfcExample7() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "telnet://192.0.2.16:80/");
        System.err.println(event);
        Assert.assertEquals("telnet", event.get("scheme"));
        Assert.assertEquals(80, event.get("port"));
        Assert.assertEquals("192.0.2.16", event.get("domain"));
        Assert.assertEquals("/", event.get("path"));
    }

    @Test
    public void testRfcExample8() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "urn:oasis:names:specification:docbook:dtd:xml:4.1.2");
        System.err.println(event);
        Assert.assertEquals("urn", event.get("scheme"));
        Assert.assertEquals("oasis:names:specification:docbook:dtd:xml:4.1.2", event.get("specific"));
    }

    @Test
    public void test_loghub_processors_UrlParser() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.UrlParser"
                , BeanChecks.BeanInfo.build("reference", String.class)
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", Object[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
