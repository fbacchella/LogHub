package loghub;

import java.util.Map;
import java.util.regex.Pattern;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;

public class Expression {

    private final static Pattern filter = Pattern.compile("\\[(.+)\\]");
    private final String expression;
    private final Script groovyScript;

    public Expression(String expression, GroovyClassLoader loader) throws InstantiationException, IllegalAccessException {
        this.expression = expression;
        @SuppressWarnings("unchecked")
        Class<Script> groovyClass = loader.parseClass(filter.matcher(expression).replaceAll("event.$1"));
        groovyScript = groovyClass.newInstance();
    }

    public Object eval(Event event, Map<String, Object> variables) {
        Binding groovyBinding = new Binding();
        variables.entrySet().stream()
        .forEach( i -> groovyBinding.setVariable(i.getKey(), i.getValue()));
        groovyBinding.setVariable("event", event);
        groovyScript.setBinding(groovyBinding);
        Object result = groovyScript.run();
        groovyScript.setBinding(new Binding());
        return result;
    }

    /**
     * @return the expression
     */
    public String getExpression() {
        return expression;
    }

}
