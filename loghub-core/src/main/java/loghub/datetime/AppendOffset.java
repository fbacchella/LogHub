package loghub.datetime;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

interface AppendOffset {

    record PatternAppendOffset(DateTimeFormatter dtf) implements AppendOffset {
        public PatternAppendOffset(String pattern) {
            this(DateTimeFormatter.ofPattern(pattern));
        }
        @Override
        public StringBuilder append(StringBuilder sb, ZonedDateTime dateTime) {
            return sb.append(dtf.format(dateTime));
        }
        @Override
        public AppendOffset withLocale(Locale locale) {
            return new PatternAppendOffset(dtf.withLocale(locale));
        }
    }

    StringBuilder append(StringBuilder sb, ZonedDateTime dateTime);
    default AppendOffset withLocale(Locale locale) {
        return this;
    }

}
