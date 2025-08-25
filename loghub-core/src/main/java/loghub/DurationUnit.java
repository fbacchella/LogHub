package loghub;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

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
    },
    DURATION {
        @Override
        public Duration from(Number n) {
            throw new UnsupportedOperationException("Can't parse a number");
        }

        public Number to(Duration p) {
            throw new UnsupportedOperationException("Can't output number");
        }
    },
    STRING {
        @Override
        public Duration from(Number n) {
            throw new UnsupportedOperationException("Can't parse a number");
        }

        @Override
        public Number to(Duration p) {
            throw new UnsupportedOperationException("Can't output number");
        }
    };

    public abstract Duration from(Number n);

    public abstract Number to(Duration p);
}
