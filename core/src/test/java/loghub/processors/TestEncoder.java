package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
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
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.encoders.Syslog;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestEncoder {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors", "loghub.encoders");
    }

    @Test
    public void testConfigurationParsing() throws IOException {
        String confile = "pipeline[encode] {\n" + "    loghub.processors.Encoder {\n" + "        encoder: loghub.encoders.Xml {\n" + "        }\n" + "    }\n" + "}\n";
        Configuration.parse(new StringReader(confile));
    }

    @Test
    public void testSyslogLine() throws IOException, ProcessorException {
        Syslog.Builder syslogBuilder = Syslog.getBuilder();
        syslogBuilder.setFormat(Syslog.Format.RFC3164);
        syslogBuilder.setDateFormat(null);
        syslogBuilder.setSeverity(new Expression(0));
        syslogBuilder.setFacility(new Expression(0));
        syslogBuilder.setMessage(new Expression("encoded message"));

        Encoder encoder = new Encoder();
        encoder.setEncoder(syslogBuilder.build());
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.setTimestamp(new Date(0));
        encoder.process(event);
        byte[] content = (byte[]) event.get("message");
        String message = new String(content, StandardCharsets.US_ASCII);
        Assert.assertEquals("<0> Thu Jan 01 00:00:00.000 1970 - encoded message", message);
    }

    @Test
    public void test_loghub_processors_Encoder() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Encoder"
                , BeanChecks.BeanInfo.build("encoder", loghub.encoders.Encoder.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
