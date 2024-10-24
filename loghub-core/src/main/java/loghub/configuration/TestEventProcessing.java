package loghub.configuration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.events.Event;
import loghub.jackson.JacksonBuilder;

public class TestEventProcessing {

    public static final String LOGGERNAME = "loghub.eventtester";
    public static final String APPENDERNAME = "eventtester";
    public static final Level LOGLEVEL = Level.INFO;

    private static final ObjectMapper jsonmapper = JacksonBuilder.get(JsonMapper.class)
                                            .setConfigurator(m -> m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                                                                   .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true))
                                            .feature(JsonWriteFeature.ESCAPE_NON_ASCII)
                                            .getMapper();

    private static final class EventJsonFormatter implements Message, StringBuilderFormattable {
        private static final ThreadLocal<DateFormat> ISO8601 = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

        private final Event event;

        public EventJsonFormatter(Event event) {
            this.event = event;
        }

        @Override
        public void formatTo(StringBuilder buffer) {
            Map<String, Object> esjson = new HashMap<>(event.size());
            esjson.putAll(event);
            esjson.put("@timestamp", ISO8601.get().format(event.getTimestamp()));

            try {
                buffer.append(jsonmapper.writeValueAsString(esjson));
            } catch (JsonProcessingException e) {
            }
        }

        @Override
        public String getFormattedMessage() {
            StringBuilder buffer = new StringBuilder();
            formatTo(buffer);
            return buffer.toString();
        }

        @Override
        public String getFormat() {
            return null;
        }

        @Override
        public Object[] getParameters() {
            return new Object[]{};
        }

        @Override
        public Throwable getThrowable() {
            return null;
        }

    }

    private TestEventProcessing() {
        // Not instantiable class
    }

    public static void check(String pipeLineTest, String configFile) {

        try {
            Properties props = Configuration.parse(configFile);

            TestEventProcessing.setAppender();

            Helpers.parallelStartProcessor(props);

            Thread t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
            t.start();

            ObjectReader reader = JacksonBuilder.get(JsonMapper.class).setTypeReference(new TypeReference<Map<?, ?>>() {}).getReader();

            MappingIterator<Map<String, Object>> i = reader.readValues(new InputStreamReader(System.in,
                    StandardCharsets.UTF_8));

            while(i.hasNext()) {
                Map<String, Object> eventMap = i.next();
                Date eventDate = null;
                if (eventMap.containsKey(Event.TIMESTAMPKEY) && eventMap.get(Event.TIMESTAMPKEY) instanceof String) {
                    TemporalAccessor ta = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:m:ss.SSSxx").parse((CharSequence) eventMap.remove(Event.TIMESTAMPKEY));

                    OffsetDateTime now;
                    // Try to resolve the time zone first
                    ZoneId zi = ta.query(TemporalQueries.zone());
                    ZoneOffset zo = ta.query(TemporalQueries.offset());
                    if ( zo != null) {
                        now = OffsetDateTime.now(zo);
                    } else if ( zi != null) {
                        now = OffsetDateTime.now(zi);
                    } else {
                        now = OffsetDateTime.now(ZoneId.systemDefault());
                    }
                    eventDate = Date.from(now.toInstant());
                }
                Event ev = props.eventsFactory.newEvent();
                ev.putAll(eventMap);
                if (eventDate != null) {
                    ev.setTimestamp(eventDate);
                }
                ev.inject(props.namedPipeLine.get(pipeLineTest), props.mainQueue, false);
            }

            Thread.currentThread().join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            System.out.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

    }

    public static void log(Event event) {
        LogManager.getLogger(LOGGERNAME).log(LOGLEVEL, new EventJsonFormatter(event));
    }

    public static void setAppender() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(true);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        // If the event logger already exists, do nothing
        if (config.getLoggers().containsKey(LOGGERNAME)) {
            return;
        }
        Layout<String> layout = PatternLayout.newBuilder().withPattern("%msg%n").withConfiguration(config).build();
        Appender jsonappender = ConsoleAppender.newBuilder()
                        .setName(APPENDERNAME)
                        .setTarget(ConsoleAppender.Target.SYSTEM_ERR)
                        .setLayout(layout)
                        .setConfiguration(config)
                        .build();
        jsonappender.start();
        config.addAppender(jsonappender);
        AppenderRef ref = AppenderRef.createAppenderRef(APPENDERNAME, null, null);
        LoggerConfig loggerConfig = LoggerConfig.newBuilder()
                                                .withAdditivity(false)
                                                .withLevel(LOGLEVEL)
                                                .withLoggerName(LOGGERNAME)
                                                .withIncludeLocation("false")
                                                .withRefs(new AppenderRef[] {ref})
                                                .withConfig(config)
                                                .build();
        loggerConfig.addAppender(jsonappender, null, null);
        config.removeLogger(LOGGERNAME);
        config.addLogger(LOGGERNAME, loggerConfig);
        ctx.updateLoggers();
    }
}
