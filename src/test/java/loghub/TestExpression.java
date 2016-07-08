package loghub;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.configuration.Properties;

public class TestExpression {

    @Test
    public void test1() throws InstantiationException, IllegalAccessException {
        VarFormatter format = new VarFormatter("${value}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.value == formatters.faaf.format(event)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("value", "a");
        Object o = expression.eval(ev, Collections.emptyMap());
        Assert.assertEquals("failed to parse expression", true, (Boolean)o);
    }

    @Test
    public void test2() throws InstantiationException, IllegalAccessException {
        VarFormatter format = new VarFormatter("${a.b}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.a.b + formatters.faaf.format(event)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", 1));
        Object o = expression.eval(ev, Collections.emptyMap());
        Assert.assertEquals("failed to parse expression", "11", (String)o);
    }

}
