package loghub.processors;

import java.util.Arrays;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
import loghub.Processor;
import loghub.configuration.Properties;

public class Etl extends Processor {

    private static final Logger logger = LogManager.getLogger();

    private String[] lvalue;
    private String expression;
    private String[] source;
    private char operator;
    private Expression script;

    @Override
    public void process(Event event) {
        switch(operator){
        case '=': {
            Object o = script.eval(event, Collections.emptyMap());
            event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, o, true);
            break;
        }
        case '-': {
            event.applyAtPath((i,j,k) -> i.remove(j), lvalue, null);
            break;
        }
        case '<': {
            Object old = event.applyAtPath((i,j,k) -> i.remove(j), source, null);
            event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, old, true);
            break;
        }
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if(operator == '=') {
            try {
                expression = expression.replaceAll("\\[(.+)\\]", "event.$1");
                script = new Expression(expression, properties.groovyClassLoader);
            } catch (CompilationFailedException e) {
                logger.error("invalid groovy expression: {}", e.getMessage());
                return false;
            } catch (InstantiationException|IllegalAccessException e) {
                logger.error("Critical groovy error: {}", e.getMessage());
                logger.throwing(Level.DEBUG, e);
                return false;
            }
        } else if (operator == '<') {
            source = expression.split("\\.");
        }
        return super.configure(properties);
    }

    @Override
    public String getName() {
        return null;
    }

    public Character getOperator() {
        return operator;
    }

    public void setOperator(Character operator) {
        this.operator = operator;
    }

    public String getLvalue() {
        return Arrays.asList(lvalue).toString();
    }

    public void setLvalue(String lvalue) {
        this.lvalue = lvalue.split("\\.");
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

}
