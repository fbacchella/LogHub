package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

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

public class TestXml {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "javax.xml", "org.xml", "loghub");
        System.setProperty("jaxp.debug", "1");
    }

    @Test
    public void extractValue() throws ProcessorException {
        Event ev = run("//c/text()", "<?xml version=\"1.0\" encoding=\"utf-16\"?><a><b><c>value</c></b></a>");
        Assert.assertEquals("value", ev.get("message"));
    }

    @Test
    public void extractList() throws ProcessorException {
        Event ev = run("//c/text()", "<?xml version=\"1.0\" encoding=\"utf-16\"?><a><b><c>value1</c><c>value2</c></b></a>");
        Assert.assertEquals(List.of("value1", "value2"), ev.get("message"));
    }

    @Test
    public void failedParse() {
        ProcessorException ex = Assert.assertThrows(ProcessorException.class, () -> run("//c/text()", "<?xml version=\"1.0\" encoding=\"utf-16\"?><a><b><c>value1</c><c>value2</c></b></a"));
        Assert.assertTrue(ex.getCause() instanceof SAXException);
    }

    private Event run(String xpath, String xml) throws ProcessorException {
        Properties p = new Properties(Collections.emptyMap());
        ParseXml.Builder parserBuilder = ParseXml.getBuilder();
        parserBuilder.setNameSpaceAware(false);
        ParseXml parser = parserBuilder.build();
        Assert.assertTrue(parser.configure(p));

        XPathExtractor.Builder extractorBuilder = XPathExtractor.getBuilder();
        extractorBuilder.setXpath(xpath);
        XPathExtractor extractor = extractorBuilder.build();
        Assert.assertTrue(extractor.configure(p));

        Event ev = factory.newEvent();
        ev.put("message", xml);
        Assert.assertTrue(parser.process(ev));
        Assert.assertTrue(extractor.process(ev));
        return  ev;
    }

    @Test
    public void test_loghub_processors_XPathExtractor() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.XPathExtractor"
                , BeanChecks.BeanInfo.build("xpath", String.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

    @Test
    public void test_loghub_processors_ParseXml() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.ParseXml"
                , BeanChecks.BeanInfo.build("nameSpaceAware", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
