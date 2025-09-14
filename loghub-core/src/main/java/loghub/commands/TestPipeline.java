package loghub.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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

    static class EventProducer implements Iterator<Event> {
        private final Queue<String> files;
        private final JsonFactory jf;
        private final JsonMapper mapper;
        private JsonParser parser;
        private final EventsFactory factory = new EventsFactory();
        private JsonToken nextToken = null;
        EventProducer(List<String> files, JsonMapper mapper) {
            this.jf = mapper.getFactory();
            this.mapper = mapper;
            this.files = new LinkedList<>(files);
        }

        @Override
        public boolean hasNext() {
            try {
                if (parser != null) {
                    nextToken = parser.nextToken();
                }
                return (nextToken != null) || ! files.isEmpty();
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        @Override
        public Event next() {
            try {
                if ((parser == null || nextToken == null) && ! files.isEmpty()) {
                    parser = jf.createParser(Helpers.fileUri(files.poll()).toURL());
                } else if (nextToken == null) {
                    throw new NoSuchElementException();
                }
                Map<String, Object> o = mapper.readValue(parser, Map.class);
                return factory.mapToEvent(ConnectionContext.EMPTY, o, true);
            } catch (IOException | DecodeException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private String configFile = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--pipeline", "-p"}, description = "Pipeline to test")
    private String pipeline = null;

    @Parameter(description = "Main parameters")
    @Getter
    private List<String> mainParams = new ArrayList<>();
    final JsonMapper mapper;
    final ObjectWriter jsonWritter;
    private int exitCode = ExitCode.OK;

    @Override
    public void reset() {
        pipeline = null;
        mainParams.clear();
    }

    public TestPipeline() {
        mapper = JacksonBuilder.get(JsonMapper.class)
                               .setConfigurator(this::mapperConfigurator)
                               .feature(JsonWriteFeature.ESCAPE_NON_ASCII)
                               .addSerializer(new EventSerializer())
                               .getMapper();
        jsonWritter = mapper.writerFor(JacksonBuilder.OBJECTREF)
                            .without(Feature.AUTO_CLOSE_TARGET)
                            .withDefaultPrettyPrinter();
    }

    private void mapperConfigurator(JsonMapper m) {
        m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
         .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Override
    public int run(PrintWriter out, PrintWriter err) {
        if (configFile == null) {
            err.println("No configuration file given");
            return ExitCode.INVALIDCONFIGURATION;
        }
        Properties props;
        try {
            props = Configuration.parse(configFile, Map.of(
                    "log4j.configURL", NullOrMissingValue.NULL,
                    "numWorkers", 1));
        } catch (IOException e) {
            err.format("Can't read configuration file %s: %s%n", configFile, Helpers.resolveThrowableException(e));
            return ExitCode.INVALIDCONFIGURATION;
        } catch (ConfigException e) {
            err.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            return ExitCode.INVALIDCONFIGURATION;
        }
        BlockingQueue<Event> testQueue = new ArrayBlockingQueue<>(10);
        props.outputQueues.put(pipeline, testQueue);

        Iterator<Event> iterator = new EventProducer(mainParams, mapper);
        Stream<Event> eventSource = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, 0),
                false
        );
        process(props, eventSource, err).forEach(e -> {
            try {
                jsonWritter.writeValue(System.out, e);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
        try {
            for (EventsProcessor ep : props.eventsprocessors) {
                ep.stopProcessing();
                ep.join(100);
            }
        } catch (InterruptedException ex) {
            // Ignore, just stop waiting
        }
        return exitCode;
    }

    @Override
    public void extractFields(BaseParametersRunner cmd) {
        cmd.getField("configFile").map(String.class::cast).ifPresent(s -> configFile = s);
    }

    Stream<Event> process(Properties props, Stream<Event> events, PrintWriter err) {
        Pipeline pipe = props.namedPipeLine.get(pipeline);
        if (pipe == null) {
            err.println("Unknown pipeline " + pipeline);
            exitCode = ExitCode.INVALIDARGUMENTS;
            return Stream.empty();
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
            err.format("Failed to start loghub: %s%n", Helpers.resolveThrowableException(e));
            return Stream.empty();
        }
        BlockingQueue<Event> testQueue = new ArrayBlockingQueue<>(10);
        props.outputQueues.put(pipeline, testQueue);
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(events.iterator(), 0),
                false
        ).map(e -> {
            e.inject(pipe, props.mainQueue, true);
            try {
                return testQueue.take();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return null;
            }
        }).takeWhile(Objects::nonNull);
    }

}
