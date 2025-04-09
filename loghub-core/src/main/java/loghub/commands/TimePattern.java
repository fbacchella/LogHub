package loghub.commands;

import java.time.format.DateTimeParseException;
import java.util.List;

import loghub.datetime.DatetimeProcessor;
import com.beust.jcommander.Parameter;

public class TimePattern implements BaseParametersRunner {

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--timepattern"}, description = "A time pattern to test")
    private String timepattern = null;

    @Override
    public int run(List<String> mainParameters) {
        if (timepattern != null) {
            DatetimeProcessor tested = DatetimeProcessor.of(timepattern);
            for (String date : mainParameters) {
                try {
                    System.out.format("%s -> %s%n", date, tested.parse(date));
                } catch (IllegalArgumentException | DateTimeParseException ex) {
                    System.out.format("%s failed%n", date);
                }
            }
            return ExitCode.OK;
        } else {
            return ExitCode.IGNORE;
        }
    }

}
