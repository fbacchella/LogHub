package loghub.datetime;

import java.time.Instant;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

public class AxisBaseRFC822 extends Scanner {

    public static final String PATTERN = "eee, d MMM yyyy HH:mm:ss Z";

    @Override
    protected ScannerRunner getScanner() {
        DatetimeProcessor dtp = PatternResolver.createNewFormatter(PATTERN);
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
