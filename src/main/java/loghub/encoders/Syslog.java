package loghub.encoders;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Syslog.Builder.class)
@CanBatch
public class Syslog extends Encoder {

    private static final byte[] BOM = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    public enum Format {
        RFC5424,
        RFC3164
    }

    public static class Builder extends Encoder.Builder<Syslog> {
        @Setter
        private Format format = Format.RFC5424;
        @Setter
        private Expression severity = new Expression("-");
        @Setter
        private Expression facility = new Expression("-");
        @Setter
        private int version = 1;
        @Setter
        private Expression hostname = new Expression("-");
        @Setter
        private Expression appname = new Expression("-");
        @Setter
        private Expression procid = new Expression("-");
        @Setter
        private Expression msgid = new Expression("-");
        /**
         * Default to the event timestamp, if it returns a number, it's millisecond since Unix epoch
         * @param the expression for the timestamp value
         */
        @Setter
        private Expression timestamp = null;
        @Setter
        private Expression message = new Expression("-");
        @Setter
        private boolean withbom = false;
        @Setter
        private String charset = StandardCharsets.US_ASCII.name();
        /**
         * If RFC3164, setting to null or empty string, not timestamp will not be displayed
         */
        @Setter
        private String dateFormat = null;
        @Setter
        private int secFrac = 3;
        @Setter
        private String defaultTimeZone = ZoneId.systemDefault().getId();
        @Override
        public Syslog build() {
            return new Syslog(this);
        }
    }

    public static Syslog.Builder getBuilder() {
        return new Syslog.Builder();
    }

    private final DatetimeProcessor dateFormatter;

    private final Format format;
    private final Charset charset;
    private final Expression severity;
    private final Expression facility;
    private final int version;
    private final Expression hostname;
    private final Expression timestamp;
    private final Expression appname;
    private final Expression procid;
    private final Expression msgid;
    private final Expression message;
    private final boolean withbom;
    private final ZoneId defaultTimeZone;

    private Syslog(Syslog.Builder builder) {
        super(builder);
        this.format = builder.format;
        this.charset = Charset.forName(builder.charset);
        this.severity = builder.severity;
        this.facility = builder.facility;
        this.version = builder.version;
        this.hostname = builder.hostname;
        this.appname = builder.appname;
        this.procid = builder.procid;
        this.msgid = builder.msgid;
        this.message = builder.message;
        this.withbom = builder.withbom;
        this.timestamp = builder.timestamp;
        this.defaultTimeZone = ZoneId.of(builder.defaultTimeZone);
        final StringBuilder timestampformat = new StringBuilder();
        if (format == Format.RFC5424) {
            timestampformat.append("yyyy-MM-dd'T'HH:mm:ss.");
            IntStream.range(0, builder.secFrac).forEach(i -> timestampformat.append("S"));
            timestampformat.append("ZZ");
        } else if (format == Format.RFC3164) {
            if (builder.dateFormat == null) {
                timestampformat.append("eee MMM dd HH:mm:ss");
                if (builder.secFrac > 0) {
                    timestampformat.append(".");
                    IntStream.range(0, builder.secFrac).forEach(i -> timestampformat.append("S"));
                }
                timestampformat.append(" yyyy");
            } else if (! builder.dateFormat.isBlank()){
                timestampformat.append(builder.dateFormat);
            }
        }
        dateFormatter = Optional.of(timestampformat)
                                .filter(s -> s.length() != 0)
                                .map(StringBuilder::toString)
                                .map(PatternResolver::createNewFormatter)
                                .map(f -> f.withDefaultZone(defaultTimeZone))
                                .orElse(null);
    }

    @Override
    public byte[] encode(Event event) throws EncodeException {
        switch (format) {
        case RFC5424:
            return formatRfc5424(event);
        case RFC3164:
            return formatRfc3164(event);
        default:
            throw new UnsupportedOperationException("Not support syslog format: " + format.name());
        }
    }

    private int getPriority(Event event) throws EncodeException, ProcessorException {
        //Resoving facility
        int realfacility;
        Object tryfacility = facility.eval(event);
        if (tryfacility instanceof Number) {
            realfacility = ((Number) tryfacility).intValue();
        } else {
            try {
                realfacility = Integer.parseInt(tryfacility.toString());
            } catch (NumberFormatException e) {
                throw new EncodeException("Invalid facility: " + tryfacility);
            }
        }
        if (realfacility < 0 || realfacility > 23) {
            throw new EncodeException("Invalid facility: " + tryfacility);
        }

        //Resolving severity
        int realseverity;
        Object tryseverity = severity.eval(event);
        if (tryseverity instanceof Number) {
            realseverity = ((Number) tryseverity).intValue();
        } else {
            try {
                realseverity = Integer.parseInt(tryseverity.toString());
            } catch (NumberFormatException e) {
                throw  new EncodeException("Invalid severity: " + tryseverity);
            }
        }
        if (realseverity < 0 || realseverity > 7) {
            throw new EncodeException("Invalid severity: " + tryseverity);
        }
        return realfacility * 8 + realseverity;
    }

    private byte[] formatRfc3164(Event event) throws EncodeException {
        try {
            List<String> parts = new ArrayList<>(3);
            parts.add("<" + getPriority((event)) + ">");
            if (dateFormatter != null) {
                parts.add(dateFormatter.print(getEventTimeStamp(event)));
            }
            if (hostname != null) {
                Optional.ofNullable(hostname.eval(event)).map(Object::toString).ifPresent(parts::add);
            }
            if (message != null) {
                Optional.ofNullable(message.eval(event)).map(Object::toString).ifPresent(parts::add);
            }
            return String.join(" ", parts).getBytes(StandardCharsets.US_ASCII);
        } catch (ProcessorException ex) {
            throw new EncodeException("Can't encode syslog message: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    private byte[] formatRfc5424(Event event) throws EncodeException {
        try {
            StringBuilder syslogline = new StringBuilder();
            syslogline.append("<");
            syslogline.append(getPriority((event)));
            syslogline.append(">");
            syslogline.append(version).append(" ");
            syslogline.append(dateFormatter.print(getEventTimeStamp(event))).append(" ");
            syslogline.append(hostname.eval(event)).append(" ");
            syslogline.append(appname.eval(event)).append(" ");
            syslogline.append(procid.eval(event)).append(" ");
            syslogline.append(msgid.eval(event)).append(" ");
            syslogline.append("[loghub]");
            String msg = Optional.ofNullable(message.eval(event)).orElse("").toString().trim();
            if (!msg.isEmpty()) {
                syslogline.append(" ");
            }
            if (!withbom) {
                syslogline.append(msg);
                return syslogline.toString().getBytes(charset);
            } else {
                byte[] prefix = syslogline.toString().getBytes(StandardCharsets.US_ASCII);
                byte[] msgbytes = msg.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.allocate(prefix.length + BOM.length + msgbytes.length);
                buffer.put(prefix).put(BOM).put(msgbytes);
                return buffer.array();
            }
        } catch (ProcessorException ex) {
            throw new EncodeException("Can't encode syslog message: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    private ZonedDateTime getEventTimeStamp(Event event) throws ProcessorException {
        if (timestamp == null) {
            return event.getTimestamp().toInstant().atZone(defaultTimeZone);
        } else {
            Object time = timestamp.eval(event);
            if (time instanceof Number) {
                return Instant.ofEpochMilli(((Number) time).longValue()).atZone(defaultTimeZone);
            } else if (time instanceof Date) {
                return ((Date)time).toInstant().atZone(defaultTimeZone);
            } else if (time instanceof Instant) {
                return ((Instant) time).atZone(defaultTimeZone);
            } else if (time instanceof ZonedDateTime) {
                return (ZonedDateTime) time;
            } else {
                throw event.buildException("Not usable timestamp " + time);
            }
        }
    }
}
