package loghub.processors;

import java.util.stream.Collectors;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Crlf.Builder.class)
public class Crlf extends FieldsProcessor {

    enum Format {
        CRLF("\r\n"),
        CR("\r"),
        LF("\n"),
        KEEP("");
        final String separator;

        Format(String separator) {
            this.separator = separator;
        }
    }

    public static class Builder extends FieldsProcessor.Builder<Crlf> {
        @Setter
        private Format format = Format.KEEP;
        @Setter
        private boolean escape;
        public Crlf build() {
            return new Crlf(this);
        }
    }
    public static Crlf.Builder getBuilder() {
        return new Crlf.Builder();
    }

    private final Format format;
    private final boolean escape;

    public Crlf(Builder builder) {
        super(builder);
        this.format = builder.format;
        this.escape = builder.escape;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (!(value instanceof String) || (format == Format.KEEP && !escape)) {
            return value;
        }
        String message = (String) value;
        if (format == Format.KEEP) {
            return message.replace("\r", "\\r").replace("\n", "\\n");
        } else {
            String separator = escape ? format.separator.replace("\r", "\\r").replace("\n", "\\n") :  format.separator ;
            return message.lines().collect(Collectors.joining(separator));
        }
    }

}
