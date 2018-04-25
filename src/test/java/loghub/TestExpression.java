package loghub;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;

public class TestExpression {

    private static Logger logger ;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression", "loghub.VarFormatter");
    }

    @Test
    public void test1() throws ExpressionException, ProcessorException {
        VarFormatter format = new VarFormatter("${value}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.value == formatters.faaf.format(event)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("value", "a");
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", true, (Boolean)o);
    }

    @Test
    public void test2() throws ExpressionException, ProcessorException {
        VarFormatter format = new VarFormatter("${b}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.a.b + formatters.faaf.format(event.a)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", 1));
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", "11", (String)o);
    }

}
