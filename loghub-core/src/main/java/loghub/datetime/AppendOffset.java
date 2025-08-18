package loghub.datetime;

import java.time.ZonedDateTime;
import java.util.Locale;

interface AppendOffset {

    StringBuilder append(StringBuilder sb, ZonedDateTime dateTime);
    default AppendOffset withLocale(Locale locale) {
        return this;
    }

}
