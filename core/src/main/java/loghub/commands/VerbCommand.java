package loghub.commands;

import com.beust.jcommander.Parameters;

public interface VerbCommand extends CommandRunner {

    default String[] getVerbs() {
        return this.getClass().getAnnotation(Parameters.class).commandNames();
    }
    default void extractFields(BaseCommand cmd) {
    }

}
