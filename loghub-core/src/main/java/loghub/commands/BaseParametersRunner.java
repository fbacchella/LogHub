package loghub.commands;

import java.util.List;
import java.util.Optional;

public interface BaseParametersRunner extends CommandLineHandler {

    int run(List<String> mainParameters);

    default <T> Optional<T> getField(String name) {
        return Optional.empty();
    }

}
