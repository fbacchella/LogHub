package loghub.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.ConnectionContext;
import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.NullOrMissingValue;
import loghub.Pipeline;
import loghub.ThreadBuilder;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.EventSerializer;
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
        if (configFile == null) {
            System.err.println("No configuration file given");
            return ExitCode.INVALIDCONFIGURATION;
        }
        Properties props;
        try {
            props = Configuration.parse(configFile, Map.of(
                    "log4j.configURL", NullOrMissingValue.NULL,
                    "numWorkers", 1));
        } catch (IOException e) {
            System.err.format("Can't read configuration file %s: %s%n", configFile, Helpers.resolveThrowableException(e));
            return ExitCode.INVALIDCONFIGURATION;
        } catch (ConfigException e) {
            System.err.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            return ExitCode.INVALIDCONFIGURATION;
        }
        Pipeline pipe = props.namedPipeLine.get(pipeline);
        if (pipe == null) {
            System.err.println("Unknown pipeline " + pipeline);
            return ExitCode.INVALIDARGUMENTS;
        }

        try {
            Helpers.parallelStartProcessor(props);
            for (EventsProcessor ep : props.eventsprocessors) {
                ep.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
                ep.start();
            }
            Helpers.waitAllThreads(props.eventsprocessors.stream());
        } catch (IllegalStateException e) {
            // Thrown by launch when a component failed to start, details are in the logs
            System.err.format("Failed to start loghub: %s%n", Helpers.resolveThrowableException(e));
            return ExitCode.FAILEDSTART;
        }
        System.err.format("outputQueues %s%n", props.outputQueues.keySet());
        BlockingQueue<Event> testQueue = new ArrayBlockingQueue(10);
        props.outputQueues.put(pipeline, testQueue);
        EventsFactory factory = new EventsFactory();

        Consumer<JsonMapper> configurator = m -> m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                                                              .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        JsonMapper mapper = JacksonBuilder.get(JsonMapper.class)
                                          .setConfigurator(configurator)
                                          .feature(JsonWriteFeature.ESCAPE_NON_ASCII)
                                          .addSerializer(new EventSerializer())
                                          .getMapper();
        ObjectWriter jsonWritter = mapper.writerFor(JacksonBuilder.OBJECTREF)
                                         .without(Feature.AUTO_CLOSE_TARGET)
                                         .withDefaultPrettyPrinter();
        JsonFactory jf = mapper.getFactory();
        for (String eventJsonFile: mainParams) {
            try {
                try (JsonParser parser = jf.createParser(Helpers.fileUri(eventJsonFile).toURL())){
                    while (parser.nextToken() != null){
                        @SuppressWarnings("unchecked")
                        Map<String, Object> o = mapper.readValue(parser, Map.class);
                        Event ev = factory.mapToEvent(ConnectionContext.EMPTY, o, true);
                        ev.inject(pipe, props.mainQueue, true);
                        Event processed = testQueue.take();
                        jsonWritter.writeValue(System.out, processed);
                    }
                }
            } catch (IOException | DecodeException e) {
                System.err.format("Can't read event JSON file file %s: %s%n", eventJsonFile, Helpers.resolveThrowableException(e));
            } catch (InterruptedException e) {
                break;
            }
        }
        try {
            for (EventsProcessor ep : props.eventsprocessors) {
                ep.stopProcessing();
                ep.join(100);
            }
        } catch (InterruptedException ex) {
            // Ignore, just stop waiting
        }
        return ExitCode.OK;
    }

    @Override
    public void extractFields(BaseParametersRunner cmd) {
        cmd.getField("configFile").map(String.class::cast).ifPresent(s -> configFile = s);
    }

}
