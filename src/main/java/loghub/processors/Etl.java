package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.Level;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.BeansManager;
import loghub.configuration.Properties;

public abstract class Etl extends Processor {

    protected String[] lvalue;

    public static class Rename extends Etl{
        private String source;
        private String[] sourcePath;
        @Override
        public void process(Event event) throws ProcessorException {
            Object old = event.applyAtPath((i,j,k) -> i.remove(j), sourcePath, null);
            if(lvalue.length == 1 && Event.TIMESTAMPKEY.equals(lvalue[0]) && old instanceof Date) {
                event.setTimestamp((Date) old);
                event.applyAtPath((i,j,k) -> i.remove(j), lvalue, null);
            } else {
                event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, old, true);
            }
        }
        @Override
        public boolean configure(Properties properties) {
            sourcePath = source.split("\\.");
            return super.configure(properties);
        }
        public String getSource() {
            return source;
        }
        public void setSource(String source) {
            this.source = source;
        }
    }

    public static class Assign extends Etl {
        private String expression;
        private Expression script;
        @Override
        public void process(Event event) throws ProcessorException {
            Object o = script.eval(event, Collections.emptyMap());
            if(lvalue.length == 1 && Event.TIMESTAMPKEY.equals(lvalue[0]) && o instanceof Date) {
                event.setTimestamp((Date) o);
            } else {
                event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, o, true);
            }
        }
        @Override
        public boolean configure(Properties properties) {
            try {
                script = new Expression(expression, properties.groovyClassLoader, properties.formatters);
            } catch (ExpressionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof CompilationFailedException) {
                    logger.error("invalid groovy expression: {}", e.getMessage());
                    return false;
                } else {
                    logger.error("Critical groovy error: {}", e.getCause().getMessage());
                    logger.throwing(Level.DEBUG, e.getCause());
                    return false;
                }
            }
            return super.configure(properties);
        }
        public String getExpression() {
            return expression;
        }
        public void setExpression(String expression) {
            this.expression = expression;
        }
    }

    public static class Convert extends Etl{
        private String className = null;
        private Class<?> clazz;
        @Override
        public void process(Event event) throws ProcessorException {
            try {
                event.applyAtPath((i,j,k) -> {
                    try {
                        Object val = i.get(j);
                        if(val == null) {
                            return null;
                        } else {
                            Object o = BeansManager.ConstructFromString(clazz, val.toString());
                            return i.put(j, o);
                        }
                    } catch (InvocationTargetException e) {
                        throw new CompletionException(e);
                    }
                }, lvalue, (Object) null, false);
            } catch (CompletionException e1) {
                throw event.buildException("unable to convert from string to " + className, (Exception)e1.getCause());
            }
        }
        @Override
        public boolean configure(Properties properties) {
            try {
                clazz = properties.classloader.loadClass(className);
            } catch (ClassNotFoundException e) {
                logger.error("class not found: {}", className);
                return false;
            }
            return super.configure(properties);
        }
        public String getClassName() {
            return className;
        }
        public void setClassName(String className) {
            this.className = className;
        }
    }


    public static class Remove extends Etl{
        @Override
        public void process(Event event) throws ProcessorException {
            event.applyAtPath((i,j,k) -> i.remove(j), lvalue, null);
        }
    }

    public abstract void process(Event event) throws ProcessorException; 

    @Override
    public String getName() {
        return null;
    }

    public String getLvalue() {
        return Arrays.asList(lvalue).toString();
    }

    public void setLvalue(String lvalue) {
        this.lvalue = lvalue.split("\\.");
    }

}
