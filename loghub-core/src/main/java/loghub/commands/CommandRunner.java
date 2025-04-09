package loghub.commands;

import com.beust.jcommander.Parameters;

public interface CommandRunner extends CommandLineHandler {

    int run();
    default String[] getVerbs() {
        return getClass().getAnnotation(Parameters.class).commandNames();
    }
    default void extractFields(BaseParametersRunner cmd) {
    }

}
