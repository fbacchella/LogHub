package loghub.encoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

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
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.senders.InMemorySender;

public class TestEvalExpression {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testone() throws EncodeException {
        EvalExpression.Builder builder = EvalExpression.getBuilder();
        builder.setCharset("UTF-16");
        builder.setFormat(new loghub.Expression("${K1}: ${K2%02d}"));
        EvalExpression encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = Tools.getEvent();
        e.put("K1", "V1");
        e.put("K2", 2);

        byte[] result = encoder.encode(e);

        String formatted = new String(result, Charset.forName("UTF-16"));
        Assert.assertEquals("Formatting failed", "V1: 02", formatted);
    }

    @Test
    public void test_loghub_encoders_EvalExpression() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.encoders.EvalExpression"
                , BeanChecks.BeanInfo.build("charset", String.class)
                , BeanChecks.BeanInfo.build("format", Expression.class)
        );
    }

}