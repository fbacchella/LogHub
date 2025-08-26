package loghub.receivers;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogBuilder;

import jdk.jfr.Configuration;
import jdk.jfr.EventType;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedObject;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;
import jdk.management.jfr.RemoteRecordingStream;
import loghub.BuildableConnectionContext;
import loghub.BuilderClass;
import loghub.DurationUnit;
import loghub.Helpers;
import loghub.cloners.Immutable;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

import static jdk.jfr.Configuration.create;
import static jdk.jfr.Configuration.getConfiguration;

@SelfDecoder
@BuilderClass(Jfr.Builder.class)
public class Jfr extends Receiver<Jfr, Jfr.Builder> {

    @Immutable
    public static class JfrContext extends BuildableConnectionContext<URI> implements Cloneable {
        @Getter
        private final URI jmxUri;
        private JfrContext(JMXServiceURL jmxUrl) {
            this.jmxUri = URI.create(jmxUrl.toString());
        }
        private JfrContext(Path jfrPath) {
            this.jmxUri = jfrPath.toUri();
        }
        private JfrContext(URI jmxUri) {
            this.jmxUri = jmxUri;
        }
        public Object clone() {
            JfrContext kc = new JfrContext(jmxUri);
            kc.setPrincipal(getPrincipal());
            return kc;
        }
        @Override
        public URI getLocalAddress() {
            return null;
        }

        @Override
        public URI getRemoteAddress() {
            return jmxUri;
        }
    }

    @Setter
    public static class Builder extends Receiver.Builder<Jfr, Jfr.Builder> {
        protected String jfrFile = "-";
        protected DurationUnit durationUnit = DurationUnit.DURATION;
        protected String jmxUrl = null;
        protected int flushInterval = 2000;
        protected String jfrConfiguration = null;
        protected String jfrConfigurationFile = null;
        protected Map<String, String> jfrSettings = Map.of();
        public Jfr build() {
            return new Jfr(this);
        }
    }
    public static Jfr.Builder getBuilder() {
        return new Jfr.Builder();
    }

    private final Path jfrFile;
    private final DurationUnit durationUnit;
    private final JMXServiceURL jmxUrl;
    private final AtomicReference<Instant> lastFlushReference;
    private final AtomicInteger failurePause;
    private final AtomicReference<RemoteRecordingStream> jfrStream;
    private volatile boolean isRunning;
    private final Map<String, String> jfrSettings;
    private final int flushInterval;
    private final JfrContext jfrContext;

    protected Jfr(Jfr.Builder builder) {
        super(builder);
        durationUnit = builder.durationUnit;
        flushInterval = builder.flushInterval;
        if (builder.jmxUrl != null) {
            try {
                jmxUrl = new JMXServiceURL(builder.jmxUrl);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid JMX URL", e);
            }
            jfrContext = new JfrContext(jmxUrl);
            jfrFile = null;
            lastFlushReference = new AtomicReference<>(Instant.now());
            failurePause = new AtomicInteger(100);
            jfrStream = new AtomicReference<>();
            try {
                 jfrSettings = resolveSettings(builder.jfrConfiguration, builder.jfrConfigurationFile, builder.jfrSettings);
            } catch (IOException | ParseException e) {
                throw new IllegalArgumentException("Unable to use the JFR configuration", e);
            }
        } else if (builder.jfrFile != null) {
            jmxUrl = null;
            jfrFile = Paths.get(Helpers.fileUri(builder.jfrFile));
            jfrContext = new JfrContext(jfrFile);
            lastFlushReference = null;
            failurePause = null;
            jfrStream = null;
            jfrSettings = null;
        } else {
            throw new IllegalArgumentException("Needs a JMX URL or jfr file path");
        }
    }

    @Override
    protected boolean isBlocking(Builder builder) {
        return builder.jmxUrl == null;
    }

    Map<String, String> resolveSettings(String jfrConfiguration, String jfrConfigurationFile, Map<String, String> settings)
            throws IOException, ParseException {
        Map<String, String> base = (jfrConfiguration == null || jfrConfiguration.isBlank()) ? new HashMap<>() :
                                           getConfiguration(jfrConfiguration).getSettings();
        if (! (jfrConfigurationFile == null || jfrConfigurationFile.isBlank())) {
            URI configUri = Helpers.fileUri(jfrConfigurationFile);
            Configuration fileConfiguration = create(new InputStreamReader(configUri.toURL().openStream(), java.nio.charset.StandardCharsets.UTF_8));
            base.putAll(fileConfiguration.getSettings());
        }
        base.putAll(settings);
        return Map.copyOf(base);
    }

    @Override
    public boolean configure(Properties properties) {
        if (jmxUrl != null) {
            properties.registerScheduledTask("JFR watch dog for " + jmxUrl, this::checkWatchdog, flushInterval);
        }
        return super.configure(properties);
    }

    @Override
    public void run() {
        if (jmxUrl == null) {
            super.run();
        } else {
            runRemoteStream();
        }
    }

    private void runRemoteStream() {
        isRunning = true;
        while (isRunning) {
            try (JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, Map.of())) {
                MBeanServerConnection conn = jmxc.getMBeanServerConnection();
                try (RemoteRecordingStream rs = new RemoteRecordingStream(conn)) {
                    logger.debug("New recording stream");
                    rs.setReuse(true);
                    rs.setMaxAge(Duration.ofSeconds(flushInterval * 2L));
                    jfrStream.set(rs);
                    rs.setSettings(jfrSettings);
                    rs.onEvent(this::handleJfrEvent);
                    rs.onError(this::handleJfrException);
                    rs.onFlush(() -> lastFlushReference.set(Instant.now()));
                    rs.start();
                }
            } catch (IOException ex) {
                logger.atError()
                      .withThrowable(logger.isDebugEnabled() ? ex : null)
                      .log("JMX connection failed: {}", () -> Helpers.resolveThrowableException(ex));
                failurePause.getAndUpdate(x -> Math.min(x * 2, 3600 * 1000));
            } catch (RuntimeException ex) {
                logger.atError()
                      .withThrowable(ex)
                      .log("JMX connection failed: {}", () -> Helpers.resolveThrowableException(ex));
                failurePause.getAndUpdate(x -> Math.min(x * 2, 3600 * 1000));
            }
            try {
                sleep(failurePause.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void handleJfrException(Throwable ex) {
        logger.atError()
              .withThrowable(ex)
              .log("JFR failure: {}", () -> Helpers.resolveThrowableException(ex));
    }

    private void checkWatchdog() {
        Instant lastFlush = lastFlushReference.get();
        if (Instant.now().toEpochMilli() - lastFlush.toEpochMilli() > flushInterval) {
            Optional.ofNullable(jfrStream.get()).ifPresent(RemoteRecordingStream::close);
        } else {
            failurePause.getAndUpdate(x -> flushInterval);
        }
    }

    private void handleJfrEvent(RecordedEvent re) {
        Event ev = convertEvent(re);
        send(ev);
    }

    @Override
    public void stopReceiving() {
        isRunning = false;
        super.stopReceiving();
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
        Event ev = getEventsFactory().newEvent(jfrContext);
        ev.putAll(fields(re));
        // Some attribute has a startTime declared
        // Detect them as flag without duration or end time
        boolean withStartTime = re.hasField("startTime");
        Duration duration = withStartTime ? Duration.ZERO : re.getDuration();
        if (withStartTime) {
            ev.remove("endTime");
            ev.remove("startTime");
            ev.remove("duration");
            ev.setTimestamp(re.getStartTime());
        } else  if (duration == Duration.ZERO || duration.isZero()) {
            ev.setTimestamp(re.getEndTime());
            ev.put("eventTime", re.getEndTime());
            ev.remove("endTime");
            ev.remove("startTime");
            ev.remove("duration");
        } else {
            ev.setTimestamp(re.getEndTime());
            ev.put("endTime", re.getEndTime());
            ev.put("startTime", re.getStartTime());
            ev.put("duration", convertDuration(re.getDuration()));
        }
        if (ev.containsKey("name") && ev.containsKey("value")) {
            String name = (String) ev.remove("name");
            Object value = ev.remove("value");
            ev.put(name, value);
        } else if (ev.containsKey("key") && ev.containsKey("value")) {
            String key = (String) ev.remove("key");
            Object value = ev.remove("value");
            ev.put(key, value);
        }
        EventType jfrEventType = re.getEventType();
        Map<String, Object> evtMap = new HashMap<>();
        Optional.ofNullable(jfrEventType.getCategoryNames()).filter(l -> ! l.isEmpty()).ifPresent(l -> evtMap.put("categories",l ));
        Optional.ofNullable(jfrEventType.getDescription()).filter(s -> ! s.isBlank()).ifPresent(s -> evtMap.put("description", s));
        Optional.ofNullable(jfrEventType.getLabel()).filter(s -> ! s.isBlank()).ifPresent(s -> evtMap.put("label", s));
        Optional.ofNullable(jfrEventType.getName()).filter(s -> ! s.isBlank()).ifPresent(s -> evtMap.put("name", s));
        ev.put("eventType", evtMap);
        return ev;
    }

    private record LocalValueDescriptor(String name, Object value) {
        LocalValueDescriptor(ValueDescriptor vd, Object o) {
            this(vd.getName(), o);
        }
    }

    private Map<String, Object> fields(RecordedObject object) {
        List<ValueDescriptor> lvd = object.getFields();
        Map<String, Object> values = HashMap.newHashMap(lvd.size());
        // startTime is a pseudo field, so skip it.
        lvd.stream()
           .map(vd -> new LocalValueDescriptor(vd, convert(object, vd)))
           .filter(e -> e.value != null && ! "startTime".equals(e.name))
           .forEach(vd -> values.put(vd.name, vd.value));
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
        return switch (descriptor.getContentType()) {
            case "jdk.jfr.Timestamp" -> object.getInstant(descriptor.getName());
            case "jdk.jfr.Timespan" -> convertDuration(object.getDuration(descriptor.getName()));
            // Return a long, to keep unsigned any type shorter that a long
            case "jdk.jfr.Unsigned" -> object.getLong(descriptor.getName());
            default -> convertFromtype(object, descriptor);
        };
    }

    private Object convertDuration(Duration duration) {
        if (durationUnit == DurationUnit.DURATION) {
            return duration;
        } else {
            try {
                return durationUnit.to(duration);
            } catch (ArithmeticException e) {
                // SECONDS_FLOAT don't throw ArithmeticException, so don't handle the double case
                return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
            }
        }
    }

    private Object convertFromtype(RecordedObject object, ValueDescriptor descriptor) {
        String name = descriptor.getName();
        return switch (descriptor.getTypeName()) {
            case "boolean" -> object.getBoolean(name);
            case "char" -> object.getChar(name);
            case "byte" -> object.getByte(name);
            case "short" -> object.getShort(name);
            case "int" -> object.getInt(name);
            case "long" -> object.getLong(name);
            case "float" -> object.getFloat(name);
            case "double" -> object.getDouble(name);
            case "java.lang.String" -> object.getString(name);
            case "jdk.types.StackTrace" -> resolveStack(object.getValue(name));
            case "java.lang.Class" -> resolveClass(object.getValue(name));
            case "jdk.types.ThreadGroup",   // RecordedThreadGroup
                 "java.lang.Thread",        // RecordedThread
                 "jdk.types.Method",        // RecordedMethod
                 "jdk.types.Module",        // RecordedObject
                 "jdk.types.Package",       // RecordedObject
                 "jdk.types.ClassLoader"    // RecordedClassLoader
                    -> resolve(object.getValue(name));
            default -> object.getValue(name);
        };
    }

    private Object resolveClass(RecordedClass clazz) {
        if (clazz == null) {
            return null;
        } else {
            List<String> fields = clazz.getFields()
                                       .stream()
                                       .map(ValueDescriptor::getName)
                                       .filter(s -> ! "name".equals(s) && ! "modifiers".equals(s) && ! "id".equals(s) && ! "hidden".equals(s))
                                       .toList();
            Map<String, Object> values = HashMap.newHashMap(fields.size());
            values.put("name", clazz.getName());
            for (String field: fields) {
                Object value = resolve(clazz.getValue(field));
                if (value != null) {
                    values.put(field, value);
                }
            }
            return values;
        }
    }

    private Object resolveStack(RecordedStackTrace stack) {
        return stack == null ? null : stack.getFrames().stream().map(this::resolve).toList();
    }

    private Object resolve(RecordedObject object) {
        return object == null ? null : fields(object);
    }

    @Override
    public String getReceiverName() {
        return "JFR:" + (jfrFile == null ? jmxUrl : jfrFile).toString();
    }

}
