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
        while (offset < length && Character.isDigit(datetime.charAt(offset)) && (lengthDigit < 0 || ((offset - startNumber) < lengthDigit))) {
            offset++;
        }
        if (startNumber == offset || (lengthDigit > 0 && (offset - startNumber) > lengthDigit)) {
            throw this.parseException("Failed to parse number");
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
                return defaultOffset;
            } else {
                if (offsetType == null) {
                    throw parseException("Zone offset unexpected");
                }
                int endZone = offset;
                int startZone = offset;
                while (endZone < length && datetime.charAt(endZone) != ' ') {
                    endZone++;
                }
                if (endZone == offset + 1 && datetime.charAt(offset) == 'Z') {
                    offset++;
                    return ZoneOffset.UTC;
                } else if (datetime.charAt(offset) == 'G' && datetime.charAt(offset) == 'M' && datetime.charAt(offset) == 'T') {
                    offset += 3;
                    if (offset == endZone) {
                        return ZoneOffset.UTC;
                    }
                } else if (datetime.charAt(offset) == 'U' && datetime.charAt(offset) == 'T' && datetime.charAt(offset) == 'C') {
                    offset += 3;
                    if (offset == endZone) {
                        return ZoneOffset.UTC;
                    }
                }
                int sign = 1;
                if (datetime.charAt(offset) == '-') {
                    sign = -1;
                    offset++;
                }
                if (offset == endZone) {
                    throw parseException("Missing or invalid time zone definition");
                }
                if (!Character.isDigit(datetime.charAt(offset))) {
                    offset = startZone;
                    return ZoneId.of(findWord());
                }
                int hour = parseInt(2);
                if (offset < endZone && datetime.charAt(offset) == ':') {
                    offset++;
                }
                int minute = 0;
                if (offset < endZone) {
                    minute = parseInt(2);
                    if (offset < endZone && datetime.charAt(offset) == ':') {
                        offset++;
                    }
                }
                int second = 0;
                if (offset < endZone) {
                    second = parseInt(2);
                    if (offset < endZone && datetime.charAt(offset) == ':') {
                        offset++;
                    }
                }
                zoneId = (hour == 0 && minute == 0 && second == 0 ) ? ZoneOffset.UTC :
                                 ZoneOffset.ofTotalSeconds(sign * (hour * 3600 + minute * 60 + second));
                return zoneId;
            }
        } catch (DateTimeException ex) {
            throw parseException(ex.getMessage());
        }
    }

    ZoneId extractZoneId(AppendOffset offsetType, ZoneId defaultOffset) {
        try {
            ZoneId zoneId;
            if (offset == length) {
                if (offsetType != null || defaultOffset == null) {
                    throw parseException("Zone ID required");
                }
                zoneId = defaultOffset;
            } else {
                if (offsetType == null) {
                    throw parseException("Zone ID unexpected");
                }
                if (offset == length - 1 && datetime.charAt(offset) == 'Z') {
                    zoneId = ZoneOffset.UTC;
                } else {
                    zoneId = ZoneId.of(findWord());
                }
            }
            return zoneId;
        } catch (DateTimeException ex) {
            throw parseException(ex.getMessage(), ex);
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

    DateTimeParseException parseException(String message, Throwable ex) {
        throw new DateTimeParseException(String.format("Failed to parse date \"%s\": %s", datetime, message), datetime, offset, ex);
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
