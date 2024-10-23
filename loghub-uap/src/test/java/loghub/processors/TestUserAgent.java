package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Collections;

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

public class TestUserAgent {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.UserAgent");
    }

    @Test
    public void test1() throws ProcessorException {
        UserAgent.Builder builder = UserAgent.getBuilder();
        builder.setField(VariablePath.of("User-Agent"));
        builder.setCacheSize(10);
        builder.setDestination(VariablePath.parse("agent"));

        UserAgent ua = builder.build();
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));

        String uaString = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";

        Event event = factory.newEvent();
        event.put("User-Agent", uaString);
        Assert.assertTrue(ua.process(event));
        Object family = event.getAtPath(VariablePath.of("agent", "userAgent", "family"));
        Assert.assertEquals("can't find user agent parsing", "Mobile Safari", family);
        Object osfamilly = event.getAtPath(VariablePath.of("agent", "os", "family"));
        Assert.assertEquals("can't find user agent parsing", "iOS", osfamilly);
    }

    @Test
    public void testDownload() {
        UserAgent.Builder builder = UserAgent.getBuilder();
        builder.setField(VariablePath.of("User-Agent"));
        builder.setCacheSize(10);
        builder.setAgentsUrl(TestUserAgent.class.getClassLoader().getResource("regexes.yaml").toString());
        builder.setDestination(VariablePath.parse("agent"));
        UserAgent ua = builder.build();
        Assert.assertTrue("configuration failed", ua.configure(new Properties(Collections.emptyMap())));
    }

    @Test
    public void test_loghub_processors_UserAgent() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.UserAgent"
                , BeanChecks.BeanInfo.build("cacheSize", Integer.TYPE)
                , BeanChecks.BeanInfo.build("agentsFile", String.class)
                , BeanChecks.BeanInfo.build("agentsUrl", String.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
                , BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE)
        );
    }

}
