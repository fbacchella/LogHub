package com.axibase.date;

import java.time.ZoneOffset;

import static com.axibase.date.DatetimeProcessorUtil.appendFormattedSecondOffset;

enum ZoneOffsetType {
    NONE {
        @Override
        StringBuilder appendOffset(StringBuilder sb, ZoneOffset offset) {
            return sb;
        }
    },
    ISO8601 {
        @Override
        StringBuilder appendOffset(StringBuilder sb, ZoneOffset offset) {
            sb.append(offset.getId());
            return sb;
        }
    },
    RFC822 {
        @Override
        StringBuilder appendOffset(StringBuilder sb, ZoneOffset offset) {
            return appendFormattedSecondOffset(true, 2, ':', offset.getTotalSeconds(), sb);
        }
    };

    abstract StringBuilder appendOffset(StringBuilder sb, ZoneOffset offset);

}
