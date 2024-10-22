package loghub.processors;

import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;

@Getter
public abstract class Etl extends Processor {

    public static class Rename extends Etl {
        public static Etl of(VariablePath lvalue, VariablePath sourcePath) {
            return new Rename(lvalue, sourcePath);
        }

        private final VariablePath sourcePath;

        private Rename(VariablePath lvalue, VariablePath sourcePath) {
            super(lvalue);
            this.sourcePath = sourcePath;
        }
        @Override
        public boolean process(Event event) {
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
    }

    public static class Assign extends Etl {
        public static Etl of(VariablePath lvalue, Expression expression) {
            return new Assign(lvalue, expression);
        }

        private final Expression expression;

        private Assign(VariablePath lvalue, Expression expression) {
            super(lvalue);
            this.expression = expression;
        }
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = expression.eval(event);
            event.putAtPath(lvalue, o);
            return true;
        }
    }

    @Getter
    public static class FromLiteral extends Etl {
        public static Etl of(VariablePath lvalue, Object litteral) {
            return new FromLiteral(lvalue, litteral);
        }

        private final Object literal;

        private FromLiteral(VariablePath lvalue, Object literal) {
            super(lvalue);
            this.literal = literal;
        }

        @Override
        public boolean process(Event event) {
            event.putAtPath(lvalue, literal);
            return true;
        }
    }

    @Getter
    public static class VarFormat extends Etl {
        public static Etl of(VariablePath lvalue, VarFormatter formatter) {
            return new VarFormat(lvalue, formatter);
        }

        private final loghub.VarFormatter formatter;

        private VarFormat(VariablePath lvalue, VarFormatter formatter) {
            super(lvalue);
            this.formatter = formatter;
        }
        @Override
        public boolean process(Event event) {
            event.putAtPath(lvalue, formatter.format(event));
            return true;
        }
    }

    @Getter
    public static class Copy extends Etl {
        public static Etl of(VariablePath lvalue, VariablePath source) {
            return new Copy(lvalue, source);
        }

        private final VariablePath source;

        private Copy(VariablePath lvalue, VariablePath source) {
            super(lvalue);
            this.source = source;
        }
        @Override
        public boolean process(Event event) {
            Object o = Expression.deepCopy(event.getAtPath(source));
            event.putAtPath(lvalue, o);
            return true;
        }
    }

    @Getter
    public static class Append extends Etl {
        public static Etl of(VariablePath lvalue, Expression expression) {
            return new Append(lvalue, expression);
        }

        private final Expression expression;

        private Append(VariablePath lvalue, Expression expression) {
            super(lvalue);
            this.expression = expression;
        }
        @Override
        public boolean process(Event event) throws ProcessorException {
            Object o = expression.eval(event);
            return event.appendAtPath(lvalue, o);
        }
    }

    public static class Convert extends Etl {
        public static Etl of(VariablePath lvalue, String className) {
            return new Convert(lvalue, className);
        }

        @Getter
        private final String className;
        private loghub.processors.Convert converter;

        private Convert(VariablePath lvalue, String className) {
            super(lvalue);
            this.className = className;
        }
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
    }

    public static class Remove extends Etl {
        public static Etl of(VariablePath lvalue) {
            return new Remove(lvalue);
        }

        private Remove(VariablePath lvalue) {
            super(lvalue);
        }
        @Override
        public boolean process(Event event) {
            event.removeAtPath(lvalue);
            return true;
        }
    }

    protected final VariablePath lvalue;

    protected Etl(VariablePath lvalue) {
        this.lvalue = lvalue;
    }

    public abstract boolean process(Event event) throws ProcessorException;

}
