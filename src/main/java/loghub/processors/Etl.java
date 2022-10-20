package loghub.processors;

import loghub.events.Event;
import loghub.events.Event.Action;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import lombok.Getter;
import lombok.Setter;

public abstract class Etl extends Processor {

    protected VariablePath lvalue;

    public static class Rename extends Etl {
        private VariablePath sourcePath;
        @Override
        public boolean process(Event event) throws ProcessorException {
            if (Boolean.TRUE.equals(event.applyAtPath(Action.CONTAINS, sourcePath, null, false))) {
                Object old = event.applyAtPath(Action.REMOVE, sourcePath, null);
                event.applyAtPath(Action.PUT, lvalue, old, true);
                return true;
            } else {
                return false;
            }
        }
        @Override
        public boolean configure(Properties properties) {
            return super.configure(properties);
        }
        public VariablePath getSource() {
            return sourcePath;
        }
        public void setSource(VariablePath source) {
            this.sourcePath = source;
        }
    }

    public static class Assign extends Etl {
        @Getter @Setter
        private Expression expression;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = expression.eval(event);
            event.applyAtPath(Action.PUT, lvalue, o, true);
            return true;
        }
    }

    public static class Append extends Etl {
        @Getter @Setter
        private Expression expression;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = expression.eval(event);
            Boolean status = (Boolean) event.applyAtPath(Action.APPEND, lvalue, o, false);
            if (status == null) {
                throw IgnoredEventException.INSTANCE;
            } else {
                return status;
            }
        }
    }

    public static class Convert extends Etl {
        private String className = null;
        private loghub.processors.Convert convert;
        @Override
        public boolean process(Event event) throws ProcessorException {
            if (Boolean.TRUE.equals(event.applyAtPath(Action.CONTAINS, lvalue, null, false))) {
                Object val = event.applyAtPath(Action.GET, lvalue, null, false);
                event.applyAtPath(Action.PUT, lvalue, convert.fieldFunction(event, val));
                return true;
            } else {
                return false;
            }
        }
        @Override
        public boolean configure(Properties properties) {
            convert = new loghub.processors.Convert();
            convert.setClassName(className);
            return convert.configure(properties) && super.configure(properties);
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

    public VariablePath getLvalue() {
        return lvalue;
    }

    public void setLvalue(VariablePath lvalue) {
        this.lvalue = lvalue;
    }

}
