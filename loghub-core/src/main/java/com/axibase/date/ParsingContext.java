package com.axibase.date;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

class ParsingContext {
    int offset;
    final int length;
    final String datetime;

    ParsingContext(String datetime) {
        this.offset = 0;
        this.length = datetime.length();
        this.datetime = datetime;
    }

    void skipSpaces() {
        while (offset < length && datetime.charAt(offset) == ' ') {
            offset++;
        }
    }

    String findWord() {
        int startWord = offset;
        while (offset < length && datetime.charAt(offset) != ' ') {
            offset++;
        }
        return datetime.substring(startWord, offset);
    }

    int parseInt(int lengthDigit) throws NumberFormatException {
        int startNumber = offset;
        while (offset < length && Character.isDigit(datetime.charAt(offset))) {
            offset++;
        }
        if (startNumber == offset || (lengthDigit > 0 && (offset - startNumber) > lengthDigit)) {
            throw new DateTimeParseException("Failed to parse number at index ", datetime, startNumber);
        }
        int result = resolveDigitByCode(datetime, startNumber);
        for (int i = startNumber + 1; i < offset; ++i) {
            result = result * 10 + resolveDigitByCode(datetime, i);
        }
        return result;
    }

    int parseNano() {
        int nanos;
        if (offset < length && datetime.charAt(offset) == '.') {
            int startPos = ++offset;
            int endPosExcl = Math.min(offset + 9, length);
            int frac = resolveDigitByCode(datetime, offset++);
            while (offset < endPosExcl) {
                int digit = datetime.charAt(offset) - '0';
                if (digit < 0 || digit > 9) {
                    break;
                }
                frac = frac * 10 + digit;
                ++offset;
            }
            nanos = DatetimeProcessorUtil.parseNanos(frac, offset - startPos);
        } else {
            nanos = 0;
        }
        return nanos;
    }

    ZoneId extractOffset(AppendOffset offsetType, ZoneId defaultOffset) {
        try {
            ZoneId zoneId;
            if (offset == length) {
                if (offsetType != null || defaultOffset == null) {
                    throw parseException("Zone offset required");
                }
                zoneId = defaultOffset;
            } else {
                if (offsetType == null) {
                    throw parseException("Zone offset unexpected");
                }
                if (offset == length - 1 && datetime.charAt(offset) == 'Z') {
                    zoneId = ZoneOffset.UTC;
                } else {
                    zoneId = ZoneOffset.of(findWord());
                }
            }
            return zoneId;
        } catch (DateTimeException ex) {
            throw this.parseException(ex.getMessage());
        }
    }

    void checkOffset(char expected){
        if (offset == length) {
            throw parseException("At end of parsing");
        }
        char found = datetime.charAt(offset++);
        if (found != expected) {
            throw parseException("Expected '" + expected + "' character but found '" + found + "'");
        }
    }

    DateTimeParseException parseException(String message) {
        throw new DateTimeParseException(String.format("Failed to parse date \"%s\": %s", datetime, message), datetime, offset);
    }


    private int resolveDigitByCode(String value, int index) {
        char c = value.charAt(index);
        int result = c - '0';
        if (result < 0 || result > 9) {
            throw parseException("Failed to parse number at index ");
        }
        return result;
    }


}
