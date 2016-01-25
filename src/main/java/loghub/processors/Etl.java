package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.BeansManager;
import loghub.configuration.Properties;

public class Etl extends Processor {

    private static final Logger logger = LogManager.getLogger();

    private String[] lvalue;
    private String expression;
    private String[] source;
    private char operator;
    private Expression script;
    private String className = null;
    private Class<?> clazz;

    @Override
    public void process(Event event) throws ProcessorException {
        switch(operator){
        case '=': {
            Object o = script.eval(event, Collections.emptyMap());
            if(lvalue.length == 1 && Event.TIMESTAMPKEY.equals(lvalue[0]) && o instanceof Date) {
                event.timestamp = (Date) o;
            } else {
                event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, o, true);
            }
            break;
        }
        case '-': {
            event.applyAtPath((i,j,k) -> i.remove(j), lvalue, null);
            break;
        }
        case '<': {
            Object old = event.applyAtPath((i,j,k) -> i.remove(j), source, null);
            if(lvalue.length == 1 && Event.TIMESTAMPKEY.equals(lvalue[0]) && old instanceof Date) {
                event.timestamp = (Date) old;
                event.applyAtPath((i,j,k) -> i.remove(j), lvalue, null);
            } else {
                event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, old, true);
            }
            break;
        }
        case '(': {
            try {
                event.applyAtPath((i,j,k) -> {
                    try {
                        Object o = BeansManager.ConstructFromString(clazz, i.get(j).toString());
                        return i.put(j, o);
                    } catch (InvocationTargetException e) {
                        throw new CompletionException(e);
                    }
                }, lvalue, (Object) null, false);
            } catch (CompletionException e1) {
                throw new ProcessorException("unable to convert from string to " + className, (Exception)e1.getCause());
            }
        }
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if(operator == '=') {
            try {
                script = new Expression(expression, properties.groovyClassLoader, properties.formatters);
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
        } else if (operator == '(') {
            try {
                clazz = properties.classloader.loadClass(className);
            } catch (ClassNotFoundException e) {
                logger.error("class not found: {}", className);
                return false;
            }
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
