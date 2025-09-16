package loghub;

import java.io.PrintWriter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import loghub.commands.ExitCode;
import loghub.commands.Parser;

public class Start {

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basic context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
        System.setProperty("log4j2.julLoggerAdapter", "org.apache.logging.log4j.jul.CoreLoggerAdapter");
    }

    public static void main(String[] args) {
        // Reset shutdown, Start can be used many time in tests.
        ShutdownTask.reset();
        Parser parser = new Parser();
        int status;
        PrintWriter out = new PrintWriter(System.out);
        PrintWriter err = new PrintWriter(System.err, true);
        try {
            JCommander jcom = parser.parse(args);
            if (parser.helpRequired()) {
                jcom.usage();
                status = ExitCode.OK;
            } else {
                status = parser.process(jcom, out, err);
            }
        } catch (ParameterException ex) {
            System.err.println("Invalid parameter: " + Helpers.resolveThrowableException(ex));
            status = ExitCode.INVALIDARGUMENTS;
        }
        if (status != ExitCode.DONTEXIT) {
            out.flush();
            err.flush();
            System.exit(status);
        }
    }

}
