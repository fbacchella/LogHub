package loghub.commands;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

public interface BaseParametersRunner extends CommandLineHandler {

    @Deprecated
    default int run(List<String> mainParameters) {
        return run(mainParameters, new PrintWriter(System.out), new PrintWriter(System.err));
    }

    default int run(List<String> mainParameters, PrintWriter out, PrintWriter err) {
        return run(mainParameters);
    }

    default <T> Optional<T> getField(String name) {
        return Optional.empty();
    }

}
