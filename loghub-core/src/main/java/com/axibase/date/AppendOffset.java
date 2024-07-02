package com.axibase.date;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

interface AppendOffset {
    class PatternAppendOffset implements AppendOffset {
        private final DateTimeFormatter dtf;

        PatternAppendOffset(String pattern) {
            dtf = DateTimeFormatter.ofPattern(pattern);
        }
        private PatternAppendOffset(DateTimeFormatter dtf) {
            this.dtf = dtf;
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
