package loghub.encoders;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.IntStream;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import lombok.Setter;

@BuilderClass(Syslog.Builder.class)
@CanBatch
public class Syslog extends Encoder {

    private static final byte[] BOM = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    private enum Format {
        RFC5424,
        RFC3164
    }

    public static class Builder extends Encoder.Builder<Syslog> {
        @Setter
        private String format = Format.RFC5424.name();
        @Setter
        private Expression severity = new Expression("-");
        @Setter
        private Expression facility = new Expression("-");;
        @Setter
        private int version = 1;
        @Setter
        private Expression hostname = new Expression("-");;
        @Setter
        private Expression appname = new Expression("-");;
        @Setter
        private Expression procid = new Expression("-");;
        @Setter
        private Expression msgid = new Expression("-");;
        @Setter
        private Expression timestamp = new Expression("-");;
        @Setter
        private Expression message = new Expression("-");;
        @Setter
        private boolean withbom = false;
        @Setter
        private String charset = StandardCharsets.US_ASCII.name();
        @Setter
        private int secFrac = 3;

        @Override
        public Syslog build() {
            return new Syslog(this);
        }
    }

    public static Syslog.Builder getBuilder() {
        return new Syslog.Builder();
    }

    private final DatetimeProcessor dateFormatter;

    @Setter
    private final Format format;
    @Setter
    private final Charset charset;
    @Setter
    private final Expression severity;
    @Setter
    private final Expression facility;
    @Setter
    private final int version;
    @Setter
    private final Expression hostname;
    @Setter
    private final Expression appname;
    @Setter
    private final Expression procid;
    @Setter
    private final Expression msgid;
    @Setter
    private final Expression timestamp;
    @Setter
    private final Expression message;
    @Setter
    private final boolean withbom;

    private Syslog(Syslog.Builder builder) {
        super(builder);
        this.format = Format.valueOf(builder.format.toUpperCase(Locale.ENGLISH));
        this.charset = Charset.forName(builder.charset);
        this.severity = builder.severity;
        this.facility = builder.facility;
        this.version = builder.version;
        this.hostname = builder.hostname;
        this.appname = builder.appname;
        this.procid = builder.procid;
        this.msgid = builder.msgid;
        this.timestamp = builder.timestamp;
        this.message = builder.message;
        this.withbom = builder.withbom;
        if (format == Format.RFC5424) {
            StringBuilder timestampformat = new StringBuilder("yyyy-MM-dd'T'HH:mm:ss.");
            IntStream.range(0, builder.secFrac).forEach(i -> timestampformat.append("S"));
            timestampformat.append("Z");
            dateFormatter = PatternResolver.createNewFormatter(timestampformat.toString()).withDefaultZone(ZoneId.of("UTC"));
        } else {
            dateFormatter = null;
        }
    }

    @Override
    public byte[] encode(Event event) throws EncodeException {
        switch (format) {
        case RFC5424:
            return formatRfc5424(event);
        default:
            throw new UnsupportedOperationException("Not support syslog format: " + format.name());
        }
    }

    private byte[] formatRfc5424(Event event) throws EncodeException {
        try {
            StringBuilder syslogline = new StringBuilder();
            syslogline.append("<");

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

            //Resoving severity
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

            syslogline.append(realfacility * 8 + realseverity);
            syslogline.append(">");
            syslogline.append(version).append(" ");
            syslogline.append(dateFormatter.print(event.getTimestamp().getTime())).append(" ");
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
        } catch (ProcessorException e) {
            throw new EncodeException("Can't encode syslog message: " + Helpers.resolveThrowableException(e), e);
        }
    }

}