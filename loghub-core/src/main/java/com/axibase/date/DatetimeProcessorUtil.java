package com.axibase.date;

import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class DatetimeProcessorUtil {

    private DatetimeProcessorUtil() {}

    static void cleanFormat(StringBuilder sb) {
        for (int last = sb.length() - 1; sb.charAt(last) == '0'; last--) {
            sb.deleteCharAt(last);
        }
        if (sb.charAt(sb.length() - 1) == '.') {
            sb.deleteCharAt(sb.length() - 1);
        }
    }

    static int parseNanos(int value, int digits) {
        return value * powerOfTen(9 - digits);
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

}
