package loghub.metrics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import loghub.Helpers;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.events.Event;
import loghub.jackson.EventSerializer;
import loghub.jackson.JacksonBuilder;
import loghub.senders.Sender;

public record EventExceptionDescription(String eventJson, CONTEXT context, String contextName, String message) {
    private static final Logger logger = LogManager.getLogger();

    private static class TruncatingStringSerializer extends StdSerializer<CharSequence> {

        private static final int MAX_BYTES = 0x1000; // 4kB
        private static final String SUFFIX = "...";

        public TruncatingStringSerializer() {
            super(CharSequence.class);
        }

        @Override
        public void serialize(CharSequence value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {

            if (value == null || value == NullOrMissingValue.NULL) {
                gen.writeNull();
                return;
            }

            String str = value.toString();
            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);

            if (bytes.length <= MAX_BYTES) {
                // Fast path: the value fits within the limit, write it as-is
                gen.writeString(str);
            } else {
                // Slow path: truncate to CONTENT_MAX_BYTES and append the suffix.
                // The truncation is byte-boundary-safe (see truncateToUtf8Bytes).
                String truncated = truncateToUtf8Bytes(str, MAX_BYTES - SUFFIX.getBytes(StandardCharsets.UTF_8).length);
                gen.writeString(truncated + SUFFIX);
            }
        }

        /**
         * Truncates {@code value} so that its UTF-8 encoding does not exceed {@code maxBytes},
         * while ensuring the result is a well-formed Unicode string.
         *
         * <p>A naive slice of the UTF-8 byte array at an arbitrary position may land in the
         * middle of a multibyte sequence (2–4 bytes for code points above U+007F).
         * To avoid producing a malformed string, the method walks backwards from {@code maxBytes}
         * until it finds a byte that is the leading byte of a UTF-8 sequence, i.e. not a
         * continuation byte (0x80–0xBF, pattern {@code 10xxxxxx}).</p>
         *
         * @param value    the original string
         * @param maxBytes the maximum number of UTF-8 bytes allowed in the result
         * @return a well-formed string whose UTF-8 encoding fits within {@code maxBytes}
         */
        private String truncateToUtf8Bytes(String value, int maxBytes) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            // Nothing to do if the encoded form already fits
            if (bytes.length <= maxBytes) {
                return value;
            }

            // Walk backwards from the cut point to find a valid sequence boundary.
            // Continuation bytes (10xxxxxx) cannot start a character, so we skip them.
            int end = maxBytes;
            while (end > 0 && isContinuationByte(bytes[end])) {
                end--;
            }

            return new String(bytes, 0, end, StandardCharsets.UTF_8);
        }

        /**
         * Returns {@code true} if {@code b} is a UTF-8 continuation byte (pattern {@code 10xxxxxx},
         * i.e. in the range 0x80–0xBF).
         *
         * <p>Such bytes are never the first byte of a code point and must not be used as
         * a truncation boundary.</p>
         *
         * @param b the byte to test
         * @return {@code true} if {@code b} is a continuation byte
         */
        private boolean isContinuationByte(byte b) {
            return (b & 0xC0) == 0x80;
        }
    }

    private static final ObjectWriter writer;
    static {
        TruncatingStringSerializer ts = new TruncatingStringSerializer();
        SimpleModule module = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "EventDescription"));
        module.addSerializer(Event.class, new EventSerializer());
        module.addSerializer(String.class, ts);
        module.addSerializer(CharSequence.class, ts);
        writer = JacksonBuilder.get(JsonMapper.class)
                               .module(module)
                               .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                               .getWriter();
    }

    private static final Function<Event, String> FORMATER = e -> {
        try {
            return writer.writeValueAsString(e);
        } catch (RuntimeException | JsonProcessingException ex) {
            logger.atError().withThrowable(ex).log("Unformattable event: {}", Helpers.resolveThrowableException(ex));
            return "Unformattable event :" + Helpers.resolveThrowableException(ex);
        }
    };

    private enum CONTEXT {
        PIPELINE(List.of("event", "pipeline", "message")) {
            @Override
            Map<String, Object> values(EventExceptionDescription descr) {
                return Map.of("event", descr.eventJson, "pipeline", descr.contextName(), "message", descr.message);
            }
        },
        SENDER(List.of("event", "sender", "message")) {
            @Override
            Map<String, Object> values(EventExceptionDescription descr) {
                return Map.of("event", descr.eventJson, "sender", descr.contextName(), "message", descr.message);
            }
        },
        ;
        private final CompositeType type;
        CONTEXT(List<String> fields) {
            try {
                String[] itemNames = fields.toArray(new String[0]);
                String[] itemDescriptions = { "Serialized event", "Then name of the receiver where the exception occurs", "Root cause message" };
                OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING};
                type = new CompositeType("FullStackCompositeData", "A composite type for MyRecord", itemNames, itemDescriptions, itemTypes);
            } catch (OpenDataException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        abstract Map<String, Object> values(EventExceptionDescription descr);
    }

    EventExceptionDescription(ProcessorException ex) {
        this(FORMATER.apply(ex.getEvent()), CONTEXT.PIPELINE, ex.getEvent().getRunningPipeline(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, Throwable ex) {
        this(FORMATER.apply(event), CONTEXT.SENDER, sender.getSenderName(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, String message) {
        this(FORMATER.apply(event), CONTEXT.SENDER, sender.getSenderName(), message);
    }

    EventExceptionDescription(Event event, Sender sender) {
        this(FORMATER.apply(event), CONTEXT.SENDER, sender.getSenderName(), "Generic failure");
    }

    CompositeDataSupport toCompositeData() {
        try {
            return new CompositeDataSupport(
                    context.type,
                    context.values(this)
            );
        } catch (OpenDataException e) {
            return null;
        }
    }

}
