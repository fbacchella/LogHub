package loghub.commands;

import java.io.PrintWriter;
import java.time.format.DateTimeParseException;
import java.util.List;

import com.beust.jcommander.Parameter;

import loghub.Helpers;
import loghub.datetime.DatetimeProcessor;

public class TimePattern implements BaseParametersRunner {

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--timepattern"}, description = "A time pattern to test")
    private String timepattern = null;

    @Override
    public void reset() {
        timepattern = null;
    }

    @Override
    public int run(List<String> mainParameters, PrintWriter o, PrintWriter e) {
        if (timepattern != null) {
            DatetimeProcessor tested = null;
            try {
                tested = DatetimeProcessor.of(timepattern);
            } catch (IllegalArgumentException ex) {
                e.format("Invalid date time pattern \"%s\": %s%n", timepattern, Helpers.resolveThrowableException(ex));
                return ExitCode.INVALIDARGUMENTS;
            }
            for (String date : mainParameters) {
                try {
                    o.format("%s -> %s%n", date, tested.parse(date));
                } catch (IllegalArgumentException | DateTimeParseException ex) {
                    o.format("failed parsing \"%s\" with \"%s\": %s%n", date, timepattern, Helpers.resolveThrowableException(ex));
                }
            }
            return ExitCode.OK;
        } else {
            return ExitCode.IGNORE;
        }
    }

}
