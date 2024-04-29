package com.axibase.date;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;
import java.util.TimeZone;

import lombok.Getter;

public class DateFormatWrapper extends DateFormat {

    @Getter
    private final DatetimeProcessor processor;

    public DateFormatWrapper(DatetimeProcessor processor) {
        this.processor = processor;
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        String formatted = processor.print(date.getTime());
        return toAppendTo.append(formatted);
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
        return Date.from(processor.parse(source).toInstant());
    }

}
