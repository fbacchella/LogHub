package loghub.processors;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(DurationConvert.Builder.class)
public class DurationConvert extends FieldsProcessor {

    public enum DurationUnit {
        NANO {
            @Override
            public Duration from(Number n) {
                return Duration.ofNanos(n.longValue());
            }
            @Override
            public Number to(Duration p) {
                return p.toNanos();
            }
        },
        MICRO {
            @Override
            public Duration from(Number n) {
                return Duration.of(n.longValue(), ChronoUnit.MICROS);
            }

            @Override
            public Number to(Duration p) {
                return p.toNanos() / 1000L;
            }
        },
        MILLI {
            @Override
            public Duration from(Number n) {
                return Duration.ofMillis(n.longValue());
            }
            @Override
            public Number to(Duration p) {
                return p.toMillis();
            }
        },
        CENTI {
            @Override
            public Duration from(Number n) {
                return Duration.ofMillis(n.longValue() * 10L);
            }
            @Override
            public Number to(Duration p) {
                return p.toMillis() / 10L;
            }
        },
        DECI {
            @Override
            public Duration from(Number n) {
                return Duration.ofMillis(n.longValue() * 100L);
            }
            @Override
            public Number to(Duration p) {
                return p.toMillis() / 100L;
            }
        },
        SECOND {
            @Override
            public Duration from(Number n) {
                return SECOND_FLOAT.from(n);
            }

            @Override
            public Number to(Duration p) {
                return p.getSeconds();
            }
        },
        SECOND_FLOAT {
            @Override
            public Duration from(Number n) {
                if (n instanceof Float || n instanceof Double) {
                    double v = n.doubleValue();
                    long seconds = ((long) v);
                    long nano = ((long) (v * 1e9) % 1_000_000_000L);
                    return Duration.ofSeconds(seconds, nano);
                } else {
                    return Duration.ofSeconds(n.longValue());
                }
            }
            @Override
            public Number to(Duration p) {
                return p.getSeconds() * 1.0 + (double) p.toNanosPart() / 1_000_000_000L;
            }
        };
        public abstract Duration from(Number n);
        public abstract Number to(Duration p);
    }

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
        Duration d;
        if (value instanceof Number) {
            d = in.from((Number) value);
        } else if (value instanceof String) {
            Number n;
            try {
                n = Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                try {
                    n = Double.parseDouble((String) value);
                } catch (NumberFormatException e2) {
                    throw event.buildException("Can't scan period " + value, e);
                }
            }
            d = in.from(n);
        } else if (value instanceof Duration) {
            d = (Duration) value;
        } else {
            throw event.buildException("Can't scan period " + value);
        }
        return out.to(d);
    }

}
