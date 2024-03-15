package loghub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import loghub.commands.CommandRunner;
import loghub.commands.BaseCommand;
import loghub.commands.ExitCode;
import loghub.commands.VerbCommand;

public class Start {

    // Prevent launching fatal shutdown many times
    // Reset in constructor
    private static boolean catchedcritical = false;

    // Keep a reference to allow external shutdown of the processing.
    // Needed by some tests
    public static Thread shutdownAction;

    // Memorize canexit, needed when loghub.Start is tested
    private static boolean testscanexit = false;
    public static Runnable hprofdump = () -> {};

    /**
     * To be called when a fatal exception is detected. It will generate a shutdown with a failure exit code.<br>
     * To be called when a thread catch any unhandled exception, or anytime when a critical exception is catched
     * @param t the uncatched exception
     */
    public static synchronized void fatalException(Throwable t) {
        // No emergency exist on InterruptedException, it's already a controlled shutdown
        if (! (t instanceof InterruptedException) && ! catchedcritical) {
            hprofdump.run();
            System.err.println("Caught a fatal exception");
            t.printStackTrace();
            shutdown();
            if (testscanexit) {
                System.exit(ExitCode.CRITICALFAILURE);
            }
        }
    }

    static synchronized void shutdown() {
        if (shutdownAction != null) {
            shutdownAction.run();
            Runtime.getRuntime().removeShutdownHook(shutdownAction);
            shutdownAction = null;
        }
    }
    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basic context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    public static void main(String[] args) {
        // Reset catched criticaly, Start can be used many time in tests.
        Start.catchedcritical = false;
        Start.shutdownAction = null;
        Start.testscanexit = true;
        JCommander.Builder jcomBuilder = JCommander.newBuilder().acceptUnknownOptions(true);
        Map<String, VerbCommand> verbs = new HashMap<>();
        List<BaseCommand> commands = new ArrayList<>();
        Consumer<ServiceLoader.Provider<CommandRunner>> resolve = p -> {
            try {
                CommandRunner cmd = p.get();
                if (cmd instanceof BaseCommand) {
                    jcomBuilder.addObject(cmd);
                    commands.add((BaseCommand) cmd);
                } else if (cmd instanceof VerbCommand) {
                    VerbCommand vcmd = (VerbCommand) cmd;
                    for (String v: vcmd.getVerbs()) {
                        verbs.put(v, vcmd);
                    }
                    jcomBuilder.addCommand(cmd);
                }
            } catch (Exception e) {
                System.out.format("Failed command: %s%n", Helpers.resolveThrowableException(e));
            }
        };
        ServiceLoader<CommandRunner> serviceLoader = ServiceLoader.load(CommandRunner.class);
        serviceLoader.stream().forEach(resolve);
        JCommander jcom = jcomBuilder.build();
        try {
            jcom.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            System.exit(ExitCode.INVALIDARGUMENTS);
        }
        for (BaseCommand dc: commands) {
            dc.getField("help", Boolean.class).ifPresent(a -> {
                if (a) {
                    jcom.usage();
                    System.exit(ExitCode.OK);
                }
            });
        }
        String parsedCommand = jcom.getParsedCommand();

        if (parsedCommand != null) {
            VerbCommand cmd = verbs.get(parsedCommand);
            for (BaseCommand dc: commands) {
                cmd.extractFields(dc);
            }
            System.exit(cmd.run(jcom.getUnknownOptions()));
        } else {
            for (BaseCommand dc: commands) {
                int status = dc.run(jcom.getUnknownOptions());
                if (status >= 0) {
                    System.exit(status);
                } else if (status == ExitCode.DONTEXIT) {
                    break;
                }
            }
        }
    }

    private Start() {
    }

}
