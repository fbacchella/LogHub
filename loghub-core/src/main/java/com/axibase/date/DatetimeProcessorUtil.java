package com.axibase.date;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class DatetimeProcessorUtil {

    static final int UNIX_EPOCH_YEAR = 1970;

    private DatetimeProcessorUtil() {}

    static void cleanFormat(StringBuilder sb) {
        for (int last = sb.length() - 1; sb.charAt(last) == '0'; last--) {
            sb.deleteCharAt(last);
        }
        if (sb.charAt(sb.length() - 1) == '.') {
            sb.deleteCharAt(sb.length() - 1);
        }
    }

    static ZoneId extractOffset(String date, int offset, AppendOffset offsetType, ZoneId defaultOffset) {
        int length = date.length();
        ZoneId zoneId;
        if (offset == length) {
            if (offsetType != null || defaultOffset == null) {
                throw new DateTimeParseException("Zone offset required", date, offset);
            }
            zoneId = defaultOffset;
        } else {
            if (offsetType == null) {
                throw new DateTimeParseException("Zone offset unexpected", date, offset);
            }
            if (offset == length - 1 && date.charAt(offset) == 'Z') {
                zoneId = ZoneOffset.UTC;
            } else {
                zoneId = ZoneOffset.of(date.substring(offset));
            }
        }
        return zoneId;
    }

    static int parseNanos(int value, int digits) {
        return value * powerOfTen(9 - digits);
    }

    static int parseInt(String value, int beginIndex, int endIndex, int valueLength) throws NumberFormatException {
        if (beginIndex < 0 || endIndex > valueLength || beginIndex >= endIndex) {
            throw new DateTimeParseException("Failed to parse number at index ", value, beginIndex);
        }
        int result = resolveDigitByCode(value, beginIndex);
        for (int i = beginIndex + 1; i < endIndex; ++i) {
            result = result * 10 + resolveDigitByCode(value, i);
        }
        return result;
    }

    static int resolveDigitByCode(String value, int index) {
        char c = value.charAt(index);
        int result = c - '0';
        if (result < 0 || result > 9) {
            throw new DateTimeParseException("Failed to parse number at index ", value, index);
        }
        return result;
    }

    static void checkOffset(String value, int offset, char expected) throws IndexOutOfBoundsException {
        char found = value.charAt(offset);
        if (found != expected) {
            throw new DateTimeParseException("Expected '" + expected + "' character but found '" + found + "'", value, offset);
        }
    }

    static ZoneOffset parseOffset(int offset, String date) {
        int length = date.length();
        ZoneOffset zoneOffset;
        if (offset == length) {
            throw new DateTimeParseException("Zone offset required", date, offset);
        } else {
            if (offset == length - 1 && date.charAt(offset) == 'Z') {
                zoneOffset = ZoneOffset.UTC;
            } else {
                zoneOffset = ZoneOffset.of(date.substring(offset));
            }
        }
        return zoneOffset;
    }

    /**
     * Return number of digits in base-10 string representation.
     * @param number Non-negative number
     * @return number of digits
     */
    @SuppressWarnings("squid:S3776") // cognitive complexity
    private static int sizeInDigits(int number) {
        int result;
        if (number < 100_000) {
            if (number < 100) {
                result = number < 10 ? 1 : 2;
            } else {
                if (number < 1000) {
                    result = 3;
                } else {
                    result = number < 10_000 ? 4 : 5;
                }
            }
        } else {
            if (number < 10_000_000) {
                result = number < 1_000_000 ? 6 : 7;
            } else {
                if (number < 100_000_000) {
                    result = 8;
                } else {
                    result = number < 1_000_000_000 ? 9 : 10;
                }
            }
        }
        return result;
    }

    static int powerOfTen(int pow) {
        switch (pow) {
            case 0: return 1;
            case 1: return 10;
            case 2: return 100;
            case 3: return 1_000;
            case 4: return 10_000;
            case 5: return 100_000;
            case 6: return 1_000_000;
            case 7: return 10_000_000;
            case 8: return 100_000_000;
            case 9: return 1_000_000_000;
        }
        for (int accum = 1, b = 10;; pow >>= 1) {
            if (pow == 1) {
                return b * accum;
            } else {
                accum *= ((pow & 1) == 0) ? 1 : b;
                b *= b;
            }
        }
    }

    static StringBuilder adjustPossiblyNegative(StringBuilder sb, int num, int positions) {
        if (num >= 0) {
            return appendNumberWithFixedPositions(sb, num, positions);
        }
        return appendNumberWithFixedPositions(sb.append('-'), -num, positions - 1);

    }

    public static StringBuilder appendNumberWithFixedPositions(StringBuilder sb, int num, int positions) {
        sb.append("0".repeat(Math.max(0, positions - sizeInDigits(num))));
        return sb.append(num);
    }

    static class ParsingContext {
        int offset;

        public ParsingContext(int offset) {
            this.offset = offset;
        }

        public ParsingContext() {
            this.offset = 0;
        }
    }

    public static int parseNano(int length, ParsingContext offsetHolder, String date) {
        int nanos;
        if (offsetHolder.offset < length && date.charAt(offsetHolder.offset) == '.') {
            int startPos = ++offsetHolder.offset;
            int endPosExcl = Math.min(offsetHolder.offset + 9, length);
            int frac = resolveDigitByCode(date, offsetHolder.offset++);
            while (offsetHolder.offset < endPosExcl) {
                int digit = date.charAt(offsetHolder.offset) - '0';
                if (digit < 0 || digit > 9) {
                    break;
                }
                frac = frac * 10 + digit;
                ++offsetHolder.offset;
            }
            nanos = parseNanos(frac, offsetHolder.offset - startPos);
        } else {
            nanos = 0;
        }
        return nanos;
    }

}
