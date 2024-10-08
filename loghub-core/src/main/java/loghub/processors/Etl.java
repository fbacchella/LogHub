package loghub.processors;

import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

public abstract class Etl extends Processor {

    protected VariablePath lvalue;

    public static class Rename extends Etl {
        private VariablePath sourcePath;
        @Override
        public boolean process(Event event) throws ProcessorException {
            if (event.containsAtPath(sourcePath)) {
                Object old = event.removeAtPath(sourcePath);
                event.putAtPath(lvalue, old);
                return true;
            } else {
                return false;
            }
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
            event.putAtPath(lvalue, o);
            return true;
        }
    }

    public static class FromLitteral extends Etl {
        @Getter @Setter
        private Object litteral;
        @Override
        public boolean process(Event event) throws ProcessorException {
            event.putAtPath(lvalue, litteral);
            return true;
        }
    }

    public static class VarFormat extends Etl {
        @Getter @Setter
        private loghub.VarFormatter formatter;
        @Override
        public boolean process(Event event) throws ProcessorException {
            event.putAtPath(lvalue, formatter.format(event));
            return true;
        }
    }

    public static class Copy extends Etl {
        @Getter @Setter
        VariablePath source;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = Expression.deepCopy(event.getAtPath(source));
            event.putAtPath(lvalue, o);
            return true;
        }
    }

    public static class Append extends Etl {
        @Getter @Setter
        private Expression expression;
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = expression.eval(event);
            return event.appendAtPath(lvalue, o);
        }
    }

    public static class Convert extends Etl {
        private String className = null;
        private loghub.processors.Convert converter;
        @Override
        public boolean process(Event event) throws ProcessorException {
            return converter.process(event);
        }
        @Override
        public boolean configure(Properties properties) {
            loghub.processors.Convert.Builder builder = loghub.processors.Convert.getBuilder();
            builder.setClassName(className);
            builder.setField(lvalue);
            builder.setDestination(lvalue);
            converter = builder.build();
            return converter.configure(properties) && super.configure(properties);
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
            event.removeAtPath(lvalue);
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
