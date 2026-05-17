package loghub.datetime;

import java.time.ZoneId;

@FunctionalInterface
public interface ParseTimeZone {
    ZoneId parse(ParsingContext context, AppendOffset offsetType, ZoneId defaultZone);
}
