package loghub.commands;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import loghub.Helpers;

public class Parser {

    private final BaseParameters baseParameters = new BaseParameters();
    private final Map<String, CommandRunner> verbs = new HashMap<>();
    private final List<CommandRunner> commands = new ArrayList<>();
    private final List<BaseParametersRunner> objects = new ArrayList<>();

    public Parser() {
        ServiceLoader<CommandLineHandler> serviceLoader = ServiceLoader.load(CommandLineHandler.class);
        serviceLoader.stream().forEach(this::resolve);
    }

    public JCommander parse(String... args) throws ParameterException {
        baseParameters.reset();
        JCommander.Builder jcomBuilder = JCommander.newBuilder()
                                                   .acceptUnknownOptions(true)
                                                   .addObject(baseParameters);
        commands.forEach(c -> {
            c.reset();
            jcomBuilder.addCommand(c);
        });
        objects.forEach(o -> {
            o.reset();
            jcomBuilder.addObject(o);
        });
        JCommander jcom = jcomBuilder.build();
        jcom.parse(args);
        return jcom;
    }

    public boolean helpRequired() {
        return baseParameters.isHelp();
    }

    void resolve(ServiceLoader.Provider<CommandLineHandler> p) {
        try {
            CommandLineHandler cmd = p.get();
            if (cmd instanceof BaseParametersRunner bpr) {
                objects.add(bpr);
            } else if (cmd instanceof CommandRunner vcmd) {
                for (String v : vcmd.getVerbs()) {
                    verbs.put(v, vcmd);
                }
                commands.add(vcmd);
            }
        } catch (Exception e) {
            System.out.format("Failed command: %s%n", Helpers.resolveThrowableException(e));
        }
    }

    @Deprecated
    public int process(JCommander jcom) {
        try (PrintWriter o = new PrintWriter(System.out); PrintWriter e = new PrintWriter(System.err)){
            return process(jcom, o, e);
        }
    }

    public int process(JCommander jcom, PrintWriter out, PrintWriter err) {
        String parsedCommand = jcom.getParsedCommand();
        if (parsedCommand != null) {
            CommandRunner cmd = verbs.get(parsedCommand);
            for (BaseParametersRunner dc : objects) {
                cmd.extractFields(dc);
            }
            return cmd.run(out, err);
        } else {
            for (BaseParametersRunner dc : objects) {
                int status = dc.run(jcom.getUnknownOptions(), out, err);
                if (status != ExitCode.IGNORE) {
                    return status;
                }
            }
            return ExitCode.INVALIDARGUMENTS;
        }
    }

}
