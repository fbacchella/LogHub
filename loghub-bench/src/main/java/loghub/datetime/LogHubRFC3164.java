package loghub.datetime;

import java.time.Instant;
import java.time.ZoneOffset;

public class LogHubRFC3164 extends Scanner {

    public static final String PATTERN = "MMM d yyyy HH:mm:ss";

    @Override
    protected ScannerRunner getScanner() {
        DatetimeProcessor dtp = DatetimeProcessor.of(PATTERN).withDefaultZone(ZoneOffset.UTC);
        return (s) -> dtp.parseInstant(s).toEpochMilli();
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
