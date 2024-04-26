package com.axibase.date;

import java.time.ZoneOffset;

import static com.axibase.date.DatetimeProcessorUtil.appendFormattedSecondOffset;

enum ZoneOffsetType {
    NONE,
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
            return appendFormattedSecondOffset(offset.getTotalSeconds(), sb);
        }
    };

    StringBuilder appendOffset(StringBuilder sb, ZoneOffset offset) {
        return sb;
    }

    public static ZoneOffsetType byPattern(String pattern) {
        if (pattern == null) {
            return NONE;
        } else if ("Z".equals(pattern) || "'Z'".equals(pattern)) {
            return RFC822;
        } else {
            return ISO8601;
        }
    }
}
