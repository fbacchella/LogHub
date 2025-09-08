package loghub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import loghub.commands.BaseParametersRunner;
import loghub.commands.CommandLineHandler;
import loghub.commands.BaseParameters;
import loghub.commands.ExitCode;
import loghub.commands.CommandRunner;

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
        BaseParameters baseParameters = new BaseParameters();
        JCommander.Builder jcomBuilder = JCommander.newBuilder().acceptUnknownOptions(false).addObject(baseParameters);
        Map<String, CommandRunner> verbs = new HashMap<>();
        List<BaseParametersRunner> commands = new ArrayList<>();
        Consumer<ServiceLoader.Provider<CommandLineHandler>> resolve = p -> {
            try {
                CommandLineHandler cmd = p.get();
                if (cmd instanceof BaseParametersRunner) {
                    jcomBuilder.addObject(cmd);
                    commands.add((BaseParametersRunner) cmd);
                } else if (cmd instanceof CommandRunner) {
                    CommandRunner vcmd = (CommandRunner) cmd;
                    for (String v : vcmd.getVerbs()) {
                        verbs.put(v, vcmd);
                    }
                    jcomBuilder.addCommand(cmd);
                }
            } catch (Exception e) {
                System.out.format("Failed command: %s%n", Helpers.resolveThrowableException(e));
            }
        };
        ServiceLoader<CommandLineHandler> serviceLoader = ServiceLoader.load(CommandLineHandler.class);
        serviceLoader.stream().forEach(resolve);
        JCommander jcom = jcomBuilder.build();
        try {
            jcom.parse(args);
        } catch (ParameterException e) {
            System.err.println("Invalid parameter: " + Helpers.resolveThrowableException(e));
            System.exit(ExitCode.INVALIDARGUMENTS);
        }
        if (baseParameters.isHelp()) {
            jcom.usage();
            System.exit(ExitCode.OK);
        }
        String parsedCommand = jcom.getParsedCommand();

        if (parsedCommand != null) {
            CommandRunner cmd = verbs.get(parsedCommand);
            for (BaseParametersRunner dc : commands) {
                cmd.extractFields(dc);
            }
            System.exit(cmd.run());
        } else {
            for (BaseParametersRunner dc : commands) {
                int status = dc.run(baseParameters.getMainParams());
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
