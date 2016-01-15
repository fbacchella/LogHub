package loghub;

import java.util.Map;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;

public class Expression {

    private final String expression;
    private final Script groovyScript;
    
    public Expression(String expression, GroovyClassLoader loader) throws InstantiationException, IllegalAccessException {
        this.expression = expression;
        @SuppressWarnings("unchecked")
        Class<Script> groovyClass = loader.parseClass(expression);
        groovyScript = groovyClass.newInstance();
    }
    
    public Object eval(Event event, Map<String, Object> variables) {
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        variables.entrySet().stream()
        .forEach( i -> groovyBinding.setVariable(i.getKey(), i.getValue()));
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
