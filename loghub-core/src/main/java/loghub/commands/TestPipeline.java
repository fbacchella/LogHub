package loghub.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.ConnectionContext;
import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.ThreadBuilder;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import lombok.Getter;
import lombok.ToString;

@Parameters(commandNames = { "test" })
@ToString
public class TestPipeline implements CommandRunner {

    private String configFile = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--pipeline", "-p"}, description = "Pipeline to test")
    private String pipeline = null;

    @Parameter(description = "Main parameters")
    @Getter
    private List<String> mainParams = new ArrayList<>();

    @Override
    public int run() {
        int exitcode = ExitCode.DONTEXIT;

        try {
            if (configFile == null) {
                System.err.println("No configuration file given");
                exitcode = ExitCode.INVALIDCONFIGURATION;
            } else {
                Properties props = Configuration.parse(configFile);
                Pipeline pipe = props.namedPipeLine.get(pipeline);

                Helpers.parallelStartProcessor(props);
                for (EventsProcessor ep : props.eventsprocessors) {
                    ep.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
                    ep.start();
                }
                Helpers.waitAllThreads(props.eventsprocessors.stream());

                EventsFactory factory = new EventsFactory();
                ObjectReader jsonReader = JacksonBuilder.get(JsonMapper.class)
                                                        .setTypeReference(new TypeReference<Map<String, Object>>() {})
                                                        .getReader();
                for (String eventJsonFile: mainParams) {
                    Map<String, Object> o = jsonReader.readValue(Helpers.fileUri(eventJsonFile).toURL());
                    Event ev = factory.mapToEvent(ConnectionContext.EMPTY, o, true);
                    ev.inject(pipe, props.mainQueue, true);
                }
            }
        } catch (ConfigException e) {
            System.err.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IOException e) {
            System.err.format("Can't read configuration file %s: %s%n", configFile, Helpers.resolveThrowableException(e));
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IllegalStateException e) {
            // Thrown by launch when a component failed to start, details are in the logs
            System.err.format("Failed to start loghub: %s%n", Helpers.resolveThrowableException(e));
            exitcode = ExitCode.FAILEDSTART;
        } catch (Exception e) {
            System.err.format("Failed to start loghub for an unhandled cause: %s%n", Helpers.resolveThrowableException(e));
            e.printStackTrace();
            exitcode = ExitCode.FAILEDSTARTCRITICAL;
        }
        return exitcode;
    }

    @Override
    public void extractFields(BaseParametersRunner cmd) {
        cmd.getField("configFile").map(String.class::cast).ifPresent(s -> configFile = s);
    }

}
