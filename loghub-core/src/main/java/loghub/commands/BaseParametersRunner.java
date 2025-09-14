package loghub.commands;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

public interface BaseParametersRunner extends CommandLineHandler {

    default int run(List<String> mainParameters) {
        return ExitCode.CRITICALFAILURE;
    }

    default int run(List<String> mainParameters, PrintWriter w) {
        return run(mainParameters);
    }

    default <T> Optional<T> getField(String name) {
        return Optional.empty();
    }

}
