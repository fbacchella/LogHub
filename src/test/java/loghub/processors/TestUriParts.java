package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

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

public class TestUriParts {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.UriParts");
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

    private Event runTest(Consumer<UriParts.Builder> configure, String field, String uriString) throws ProcessorException {
        UriParts.Builder builder = UriParts.getBuilder();
        configure.accept(builder);
        UriParts parts = builder.build();
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
    public void test3() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/foo?key1=val1&key2=val2#frag");
        Assert.assertEquals("/foo", event.get("path"));
        Assert.assertFalse(event.containsKey("extension"));
    }

    @Test
    public void test4() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:mypassword@www.example.com:80/foo.?key1=val1&key2=val2#frag");
        Assert.assertEquals("/foo.", event.get("path"));
        Assert.assertFalse(event.containsKey("extension"));
    }

    @Test
    public void test5() throws ProcessorException {
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
    public void test6() throws ProcessorException {
        Event event = runTest(b -> {
                    b.setField(VariablePath.of(new String[] {"original"}));
                    b.setInPlace(true);
                }, "original",
                "http://myusername:@www.example.com:80/foo.gif?key1=val1&key2=val2#frag");
        Assert.assertEquals("myusername", event.get("username"));
        Assert.assertEquals("", event.get("password"));
    }

    @Test
    public void test_loghub_processors_UriParts() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.UriParts"
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", Object[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
        );
    }

}
