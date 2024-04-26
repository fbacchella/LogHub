package com.axibase.date;

import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Map;

class ShortMonthDateTimeProcessor extends AbstractMonthDateTimeProcessor {
    ShortMonthDateTimeProcessor(Locale locale, ZoneId zoneId) {
        super(locale, TextStyle.SHORT, TextStyle.SHORT_STANDALONE, zoneId);
    }

    private ShortMonthDateTimeProcessor(DateTimeFormatter formatter, Map<String, Month> monthMap, ZoneId defaultZone) {
        super(formatter, monthMap, defaultZone);
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return formatter.getLocale().equals(locale) ? this : new ShortMonthDateTimeProcessor(locale, defaultZone);
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return defaultZone.equals(zoneId) ? this : new ShortMonthDateTimeProcessor(formatter, monthMap, zoneId);
    }
}
