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
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
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
        Assert.assertEquals("login", values.get("user"));
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

    @Test
    public void test_loghub_processors_UrlParser() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.UrlParser"
                , BeanChecks.BeanInfo.build("reference", String.class)
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
        );
    }

}
