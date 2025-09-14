package loghub.commands;

import java.io.PrintWriter;

import com.beust.jcommander.Parameters;

public interface CommandRunner extends CommandLineHandler {

    @Deprecated
    default int run() {
        return ExitCode.CRITICALFAILURE;
    }

    default int run(PrintWriter out, PrintWriter err) {
        return run();
    }

    default String[] getVerbs() {
        return getClass().getAnnotation(Parameters.class).commandNames();
    }
    default void extractFields(BaseParametersRunner cmd) {
    }

}
