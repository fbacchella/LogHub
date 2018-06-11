package loghub.processors;

import java.lang.reflect.InvocationTargetException;

import org.apache.logging.log4j.Level;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Event.Action;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.BeansManager;
import loghub.configuration.Properties;

public abstract class Etl extends Processor {

    protected String[] lvalue;

    public static class Rename extends Etl{
        private String[] sourcePath;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object old = event.applyAtPath(Action.REMOVE, sourcePath, null);
            event.applyAtPath(Action.PUT, lvalue, old, true);
            return true;
        }
        @Override
        public boolean configure(Properties properties) {
            return super.configure(properties);
        }
        public String[] getSource() {
            return sourcePath;
        }
        public void setSource(String[] source) {
            this.sourcePath = source;
        }
    }

    public static class Assign extends Etl {
        private String expression;
        private Expression script;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = script.eval(event);
            event.applyAtPath(Action.PUT, lvalue, o, true);
            return true;
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

    public static class Convert extends Etl {
        private String className = null;
        private Class<?> clazz;
        @Override
        public boolean process(Event event) throws ProcessorException {
            try {
                Object val = event.applyAtPath(Action.GET, lvalue, null, false);
                if(val != null) {
                    Object o = BeansManager.ConstructFromString(clazz, val.toString());
                    event.applyAtPath(Action.PUT, lvalue, o);
                }
                return true;
            } catch (InvocationTargetException e) {
                throw event.buildException("unable to convert from string to " + className, e);
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


    public static class Remove extends Etl {
        @Override
        public boolean process(Event event) throws ProcessorException {
            event.applyAtPath(Action.REMOVE, lvalue, null);
            return true;
        }
    }

    public abstract boolean process(Event event) throws ProcessorException; 

    public String[] getLvalue() {
        return lvalue;
    }

    public void setLvalue(String[] lvalue) {
        this.lvalue = lvalue;
    }

}
