package loghub.processors;

import java.time.DateTimeException;
import java.time.Duration;

import loghub.BuilderClass;
import loghub.DurationUnit;
import loghub.IgnoredEventException;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(DurationConvert.Builder.class)
public class DurationConvert extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<DurationConvert> {
        private DurationUnit in = DurationUnit.SECOND;
        private DurationUnit out = DurationUnit.SECOND;
        public DurationConvert build() {
            return new DurationConvert(this);
        }
    }
    public static DurationConvert.Builder getBuilder() {
        return new DurationConvert.Builder();
    }

    private final DurationUnit in;
    private final DurationUnit out;

    private DurationConvert(DurationConvert.Builder builder) {
        super(builder);
        in = builder.in;
        out = builder.out;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        // input value was expected to be a duration
        if (in == DurationUnit.DURATION && ! (value instanceof Duration)) {
            throw IgnoredEventException.INSTANCE;
        }
        Duration d;
        if (value instanceof Number) {
            d = in.from((Number) value);
        } else if (value instanceof String) {
            if (in == DurationUnit.STRING) {
                try {
                    d = Duration.parse((String) value);
                } catch (DateTimeException | ArithmeticException e) {
                    throw event.buildException("Can't parse duration " + value, e);
                }
            } else {
                Number n;
                try {
                    n = Long.parseLong((String) value);
                } catch (NumberFormatException e) {
                    try {
                        n = Double.parseDouble((String) value);
                    } catch (NumberFormatException e2) {
                        throw event.buildException("Can't parse duration " + value, e);
                    }
                }
                d = in.from(n);
            }
        } else if (value instanceof Duration) {
            // Can't take Period any way, conversions fails
            d = (Duration) value;
        } else {
            throw event.buildException("Can't resolve period " + value);
        }
        if (out == DurationUnit.DURATION) {
            return d;
        } else if (out == DurationUnit.STRING) {
            return d.toString();
        } else {
            return out.to(d);
        }
    }

}
