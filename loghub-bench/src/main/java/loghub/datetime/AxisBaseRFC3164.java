package loghub.datetime;

import java.time.Instant;
import java.time.ZoneOffset;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

public class AxisBaseRFC3164 extends Scanner {

    public static final String PATTERN = "MMM d yyyy HH:mm:ss";

    @Override
    protected ScannerRunner getScanner() {
        DatetimeProcessor dtp = PatternResolver.createNewFormatter(PATTERN).withDefaultZone(ZoneOffset.UTC);
        return dtp::parseMillis;
    }

    @Override
    protected long getSourceTimestamp() {
        // A second precision for the time stamp
        return Instant.ofEpochMilli(super.getSourceTimestamp()).getEpochSecond() * 1000;
    }

    @Override
    public String getToParse(long timestamp) {
        return PatternResolver.createNewFormatter(PATTERN).print(timestamp);
    }

}
