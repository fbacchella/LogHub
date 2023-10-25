package loghub.receivers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogBuilder;

import jdk.jfr.Timespan;
import jdk.jfr.Timestamp;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedClassLoader;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedObject;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordedThreadGroup;
import jdk.jfr.consumer.RecordingFile;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.events.Event;
import lombok.Setter;

@SelfDecoder
@BuilderClass(Jfr.Builder.class)
public class Jfr extends Receiver<Jfr, Jfr.Builder> {

    public static class Builder extends Receiver.Builder<Jfr, Jfr.Builder> {
        @Setter
        protected String jfrFile = "-";
        @Setter
        protected ChronoUnit periodUnit = null;
        public Jfr build() {
            return new Jfr(this);
        }
    }

    public static Jfr.Builder getBuilder() {
        return new Jfr.Builder();
    }

    private final Path jfrFile;
    private final Function<Duration, Object> convertDuration;

    protected Jfr(Jfr.Builder builder) {
        super(builder);
        jfrFile = Paths.get(Helpers.fileUri(builder.jfrFile));
        convertDuration = builder.periodUnit != null ? d -> d.get(builder.periodUnit) : d -> d;
    }

    @Override
    protected Stream<Event> getStream() {
        try {
            RecordingFile recordingFile = new RecordingFile(jfrFile);
            AtomicReference<RecordedEvent> refRe = new AtomicReference<>();
            if (setRecordedEvent(recordingFile, refRe)) {
                return Stream.iterate(refRe.get(), e -> setRecordedEvent(recordingFile, refRe), e -> refRe.get())
                             .map(this::convertEvent);
            } else {
                return Stream.empty();
            }
        } catch (IOException ex) {
            LogBuilder lb = logger.atError();
            lb = logger.isDebugEnabled() ? lb.withThrowable(ex) : lb;
            lb.log("IO exception when reading {}: {}", () -> jfrFile, () -> Helpers.resolveThrowableException(ex));
            return Stream.empty();
        }
    }

    private boolean setRecordedEvent(RecordingFile recordingFile, AtomicReference<RecordedEvent> refRe) {
        if (recordingFile.hasMoreEvents()) {
            try {
                refRe.set(recordingFile.readEvent());
                return true;
            } catch (RuntimeException ex) {
                logger.atError().withThrowable(ex).log("Runtime exception when reading {}: {}", () -> jfrFile, () -> Helpers.resolveThrowableException(ex));
                return false;
            } catch (IOException ex) {
                LogBuilder lb = logger.atError();
                lb = logger.isDebugEnabled() ? lb.withThrowable(ex) : lb;
                lb.log("IO exception when reading {}: {}", () -> jfrFile, () -> Helpers.resolveThrowableException(ex));
                closeRecordingFile(recordingFile);
                return false;
            }
        } else {
            closeRecordingFile(recordingFile);
            return false;
        }
    }

    private void closeRecordingFile(RecordingFile recordingFile) {
        try {
            recordingFile.close();
        } catch (IOException e) {
            logger.atDebug().withThrowable(e).log("IO exception when closing {}: {}", () -> this.jfrFile, () -> Helpers.resolveThrowableException(e));
        }
    }

    private Event convertEvent(RecordedEvent re) {
        Event ev = getEventsFactory().newEvent();
        ev.putAll(fields(re));
        Duration duration = re.getDuration();
        if (duration.toNanos() == 0) {
            ev.put("eventTime", re.getEndTime());
            ev.remove("endTime");
            ev.remove("startTime");
            ev.remove("duration");
        } else {
            ev.put("endTime", re.getEndTime());
            ev.put("startTime", re.getStartTime());
            ev.put("duration", convertDuration.apply(re.getDuration()));
        }
        ev.put("eventType", re.getEventType().getName());
        ev.setTimestamp(re.getEndTime());
        return ev;
    }

    private Map<String, Object> fields(RecordedObject object) {
        Map<String, Object> values = new HashMap<>();
        object.getFields().stream()
                .map(vd -> Map.entry(vd.getName(), Optional.ofNullable(convert(object, vd))))
                .filter(e -> e.getValue().isPresent())
                .forEach(e -> values.put(e.getKey(), e.getValue().get()));
        return values;
    }

    private Object convert(RecordedObject object, ValueDescriptor descriptor) {
        if (descriptor.getContentType() != null) {
            return convertFromContentType(object, descriptor);
        } else {
            return convertFromtype(object, descriptor);
        }
    }

    private Object convertFromContentType(RecordedObject object, ValueDescriptor descriptor) {
        Object value = object.getValue(descriptor.getName());
        if (value == null) {
            return null;
        } else {
            switch (descriptor.getContentType()) {
            case "jdk.jfr.Timestamp":
                return resolveTimeStamp(descriptor, (Long) value);
            case "jdk.jfr.Timespan":
                return resolveTimeSpan(descriptor, (Long) value);
            default:
                return convertFromtype(object, descriptor);
            }
        }
    }

    private Object resolveTimeStamp(ValueDescriptor descriptor, long value) {
        String unit = descriptor.getAnnotation(Timestamp.class).value();
        switch (unit) {
        case Timestamp.MILLISECONDS_SINCE_EPOCH:
            return Instant.ofEpochMilli(value);
        case Timestamp.TICKS:
            return value;
        default:
            throw new IllegalArgumentException(unit);
        }
    }

    private Object resolveTimeSpan(ValueDescriptor descriptor, long value) {
        String unit = descriptor.getAnnotation(Timespan.class).value();
        Function<ChronoUnit, Object> resolveDuration = c-> convertDuration.apply(Duration.of(value, c));
        switch (unit) {
        case Timespan.MICROSECONDS:
        return resolveDuration.apply(ChronoUnit.MICROS);
        case Timespan.MILLISECONDS:
            return resolveDuration.apply(ChronoUnit.MILLIS);
        case Timespan.NANOSECONDS:
            return resolveDuration.apply(ChronoUnit.NANOS);
        case Timespan.SECONDS:
            return resolveDuration.apply(ChronoUnit.SECONDS);
        case Timespan.TICKS:
            return value;
        default:
            throw new IllegalArgumentException(unit);
        }
    }

    private Object convertFromtype(RecordedObject object, ValueDescriptor descriptor) {
        Object value = object.getValue(descriptor.getName());
        if (value == null) {
            return null;
        } else {
            switch (descriptor.getTypeName()) {
            case "java.lang.Class": return resolve((RecordedClass) value);
            case "java.lang.Thread": return resolve((RecordedThread) value);
            case "jdk.types.StackTrace": return resolve((RecordedStackTrace) value);
            case "jdk.types.ClassLoader": return resolve((RecordedClassLoader) value);
            case "jdk.types.Method": return resolve((RecordedMethod) value);
            case "jdk.types.ThreadGroup": return resolve((RecordedThreadGroup) value);
            case "jdk.types.VirtualSpace":
            case "jdk.types.MetaspaceSizes":
            case "jdk.types.G1EvacuationStatistics":
            case "jdk.types.Module":
            case "jdk.types.Package":
            case "jdk.types.OldObject":
                return resolve((RecordedObject) value);
            default:
                return value;
            }
        }
    }

    private Object resolve(RecordedStackTrace stack) {
        return stack.getFrames().stream().map(this::resolve).collect(Collectors.toList());
    }

    private Object resolve(RecordedObject object) {
        return fields(object);
    }

    @Override
    public String getReceiverName() {
        return "JFR:" + jfrFile;
    }

}
