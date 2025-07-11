package loghub.kafka.range;

import java.util.Objects;

import lombok.Getter;

@Getter
class LongRange implements Comparable<LongRange> {
    private final long start;
    private final long end;
    static final LongRange EMPTY = new LongRange(Long.MAX_VALUE, Long.MIN_VALUE);

    private LongRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    long size() { return this == EMPTY ? 0 : end - start + 1; }

    boolean isContiguousWith(LongRange other) {
        return this.end >= other.start - 1 && other.end >= this.start - 1;
    }

    LongRange mergeWith(LongRange other) {
        if (this == EMPTY) {
            return other;
        } else if (other == EMPTY) {
            return this;
        } else if (other.start >= this.start && other.end <= this.end) {
            return this;
        } else if (this.start >= other.start && this.end <= other.end) {
            return other;
        } else {
            return new LongRange(
                    Math.min(this.start, other.start),
                    Math.max(this.end, other.end)
            );
        }
    }

    boolean contains(long value) {
        return value >= start && value <= end;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof LongRange)) {
            return false;
        } else {
            LongRange range = (LongRange) obj;
            return start == range.start && end == range.end;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        if (start > end) {
            return "EMPTY";
        } else {
            return "[" + start + "—" + end + "]";
        }
    }

    static LongRange of(long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("Start must be <= end");
        } else {
            return new LongRange(start, end);
        }
    }

    static LongRange of(long value) {
        return new LongRange(value, value);
    }

    static LongRange of() {
        return EMPTY;
    }

    @Override
    public int compareTo(LongRange other) {
        if (equals(other)) {
            return 0;
        } else if (other == EMPTY) {
            return 1;
        } else {
            if (this.start == other.start) {
                return this.end < other.end ? -1 : 1;
            } else {
                return this.start < other.start ? -1 : 1;
            }
        }
    }
}
