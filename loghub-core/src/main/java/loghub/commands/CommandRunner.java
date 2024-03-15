package loghub.commands;

import java.util.List;

public interface CommandRunner {
    int run(List<String> unknownOptions);
}
