package loghub.datetime;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class JavaDtfRFC3164 extends Scanner {

    public static final String PATTERN = "MMM d yyyy HH:mm:ss";

    @Override
    protected ScannerRunner getScanner() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(PATTERN).withZone(ZoneOffset.UTC);
        return s -> dtf.parse(s).query(Instant::from).getLong(ChronoField.INSTANT_SECONDS) * 1000;
    }

    @Override
    protected long getSourceTimestamp() {
        // A second precision for the time stamp
        return Instant.ofEpochMilli(super.getSourceTimestamp()).getEpochSecond() * 1000;
    }

    @Override
    public String getToParse(long timestamp) {
        return DatetimeProcessor.of(PATTERN).print(Instant.ofEpochMilli(timestamp));
    }

}
