package loghub.datetime;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

public class AxisBaseISO8601 extends Scanner {

    public static final String PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSXXX";

    @Override
    protected ScannerRunner getScanner() {
        DatetimeProcessor dtp = PatternResolver.createNewFormatter(PATTERN);
        return dtp::parseMillis;
    }

    @Override
    public String getToParse(long timestamp) {
        return PatternResolver.createNewFormatter(PATTERN).print(timestamp);
    }

}
