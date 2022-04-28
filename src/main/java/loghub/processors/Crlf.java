package loghub.processors;

import java.util.Locale;
import java.util.stream.Collectors;

import loghub.Event;
import loghub.ProcessorException;
import lombok.Getter;
import lombok.Setter;

public class Crlf extends FieldsProcessor {

    private enum Format {
        CRLF("\r\n"),
        CR("\r"),
        LF("\n"),
        KEEP("");
        final String separator;

        Format(String separator) {
            this.separator = separator;
        }
    }

    private Format format = Format.KEEP;
    @Getter @Setter
    private boolean escape;

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (!(value instanceof String) || (format == Format.KEEP && !escape)) {
            return value;
        }
        String message = (String) value;
        if (format == Format.KEEP && escape) {
            return message.replace("\r", "\\r").replace("\n", "\\n");
        } else {
            String separator = escape ? format.separator.replace("\r", "\\r").replace("\n", "\\n") :  format.separator ;
            return message.lines().collect(Collectors.joining(separator));
        }
    }

    public String getFormat() {
        return format.name();
    }

    public void setFormat(String format) {
        if (format == null || format.isBlank()) {
            this.format = Format.KEEP;
        } else {
            this.format = Format.valueOf(format.toUpperCase(Locale.ENGLISH));
        }
    }

}
