package loghub.commands;

import java.util.Optional;

public interface BaseCommand extends CommandRunner {

    <T> Optional<T> getField(String name, Class<T> tClass);

}
