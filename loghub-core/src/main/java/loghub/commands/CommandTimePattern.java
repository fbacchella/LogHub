package loghub.commands;

import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;

import loghub.datetime.DatetimeProcessor;
import com.beust.jcommander.Parameter;

public class CommandTimePattern  implements BaseCommand {

    @Parameter(names = {"--timepattern"}, description = "A time pattern to test")
    private String timepattern = null;

    @Override
    public int run(List<String> unknownOptions) {
        if (timepattern != null) {
            DatetimeProcessor tested = DatetimeProcessor.of(timepattern);
            for (String date: unknownOptions) {
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

    @Override
    public <T> Optional<T> getField(String name, Class<T> tClass) {
        return Optional.empty();
    }
}
