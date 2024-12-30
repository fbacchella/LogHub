package loghub.receivers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogBuilder;

import jdk.jfr.Category;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedObject;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.events.Event;
import lombok.Setter;

@SelfDecoder
@BuilderClass(Jfr.Builder.class)
@Blocking
public class Jfr extends Receiver<Jfr, Jfr.Builder> {

    public enum DURATION_FORMAT {
        KEEP,
        NANOS,
        MICROS,
        MILLIS,
        SECONDS,
        SECONDS_FLOAT,
    }

    @Setter
    public static class Builder extends Receiver.Builder<Jfr, Jfr.Builder> {
        protected String jfrFile = "-";
        protected DURATION_FORMAT durationUnit = null;
        public Jfr build() {
            return new Jfr(this);
        }
    }

    public static Jfr.Builder getBuilder() {
        return new Jfr.Builder();
    }

    private final Path jfrFile;
    private final DURATION_FORMAT durationUnit;

    protected Jfr(Jfr.Builder builder) {
        super(builder);
        jfrFile = Paths.get(Helpers.fileUri(builder.jfrFile));
        durationUnit = builder.durationUnit;
    }

    @Override
    protected Stream<Event> getStream() {
        try {
            RecordingFile recordingFile = new RecordingFile(jfrFile);
            AtomicReference<RecordedEvent> refRe = new AtomicReference<>();
            if (setRecordedEvent(recordingFile, refRe)) {
                return Stream.iterate(refRe.get(), e -> setRecordedEvent(recordingFile, refRe), e -> refRe.get())
                             .map(this::convertEvent)
                             .parallel();
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
            ev.put("duration", convertDuration(re.getDuration()));
        }
        ev.put("eventType", re.getEventType().getName());
        if (ev.containsKey("name") && ev.containsKey("value")) {
            String name = (String) ev.remove("name");
            Object value = ev.remove("value");
            ev.put(name, value);
        } else if (ev.containsKey("key") && ev.containsKey("value")) {
            String key = (String) ev.remove("key");
            Object value = ev.remove("value");
            ev.put(key, value);
        }
        ev.setTimestamp(re.getEndTime());
        Optional.ofNullable(re.getEventType().getAnnotation(Category.class))
                .map(c -> String.join("/", c.value()))
                .ifPresent(c -> ev.putMeta("category", c));
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
        switch (descriptor.getContentType()) {
        case "jdk.jfr.Timestamp":
            return object.getInstant(descriptor.getName());
        case "jdk.jfr.Timespan":
            return convertDuration(object.getDuration(descriptor.getName()));
        default:
            return convertFromtype(object, descriptor);
        }
    }

    private Object convertDuration(Duration duration) {
        try {
            switch (durationUnit) {
            case KEEP:
                return duration;
            case NANOS:
                return duration.toNanos();
            case MICROS:
                return duration.toNanos() / 1000L;
            case MILLIS:
                return duration.toMillis();
            case SECONDS:
                return duration.toSeconds();
            case SECONDS_FLOAT:
                return duration.toSeconds() + (double) duration.toNanosPart() / 1_000_000_000;
            default:
                throw new IllegalArgumentException(durationUnit.toString());
            }
        } catch (ArithmeticException e) {
            // SECONDS_FLOAT don't throw ArithmeticException, so don't handle the double case
            return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
         }
    }

    private Object convertFromtype(RecordedObject object, ValueDescriptor descriptor) {
        String name = descriptor.getName();
        switch (descriptor.getTypeName()) {
        case "boolean":
            return object.getBoolean(name);
        case "char":
            return object.getChar(name);
        case "byte":
            return object.getByte(name);
        case "short":
            return object.getShort(name);
        case "int":
            return object.getInt(name);
        case "long":
            return object.getLong(name);
        case "float":
            return object.getFloat(name);
        case "double":
            return object.getDouble(name);
        case "jdk.types.StackTrace":
            return resolveStack(object.getValue(name));
        default:
            Object value = object.getValue(name);
            if (value instanceof RecordedObject) {
                return resolve((RecordedObject) value);
            } else {
                return value;
            }
        }
    }

    private Object resolveStack(RecordedStackTrace stack) {
        return stack == null ? null : stack.getFrames().stream().map(this::resolve).collect(Collectors.toList());
    }

    private Object resolve(RecordedObject object) {
        return object == null ? null : fields(object);
    }

    @Override
    public String getReceiverName() {
        return "JFR:" + jfrFile;
    }

}
