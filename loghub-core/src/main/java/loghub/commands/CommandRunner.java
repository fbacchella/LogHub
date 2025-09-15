package loghub.commands;

import java.io.PrintWriter;
import java.util.List;

import com.beust.jcommander.Parameters;

public interface CommandRunner extends CommandLineHandler {

    @Deprecated
    default int run() {
        return run(List.of(), new PrintWriter(System.out), new PrintWriter(System.err));
    }

    default int run(PrintWriter out, PrintWriter err) {
        return run(List.of(), out, err);
    }

    default int run(List<String> mainArguments, PrintWriter out, PrintWriter err) {
        return run(out, err);
    }

    default String[] getVerbs() {
        return getClass().getAnnotation(Parameters.class).commandNames();
    }
    default void extractFields(BaseParametersRunner cmd) {
    }

}
