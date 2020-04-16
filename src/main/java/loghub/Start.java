package loghub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.jmx.JmxService;
import loghub.processors.FieldsProcessor;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.senders.Sender;

public class Start {

    @Parameter(names = {"--configfile", "-c"}, description = "File")
    String configFile = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help;

    @Parameter(names = {"--test", "-t"}, description = "Test mode")
    boolean test = false;

    @Parameter(names = {"--stats", "-s"}, description = "Dump stats on exit")
    boolean dumpstats = false;

    @Parameter(names = "--canexit", description = "Prevent call to System.exit(), for JUnit tests only", hidden = true)
    boolean canexit = true;

    @Parameter(names = {"--testprocessor", "-p"}, description = "A field processor to test")
    String testedprocessor = null;

    @Parameter(names = {"--sign", "-S"}, description = "Sign a JWT token")
    boolean sign = false;

    @Parameter(names = {"--signfile", "-F"}, description = "The jwt token to sign")
    String signfile = null;

    String pipeLineTest = null;
    int exitcode = 0;

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basis context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    private static final Logger logger = LogManager.getLogger();

    // It's exported for tests
    private static Thread shutdownAction;

    static public void main(final String[] args) {
        Start main = new Start();
        JCommander jcom = JCommander
                        .newBuilder()
                        .addObject(main)
                        .build();

        try {
            jcom.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
        }
        if (main.help) {
            jcom.usage();
            System.exit(0);
        }
        main.configure();
    }

    private void configure() {

        if (testedprocessor != null) {
            test = true;
            dumpstats = false;
        }
        if (pipeLineTest != null) {
            TestEventProcessing.check(pipeLineTest, configFile);
            exitcode = 0;
        }
        if (dumpstats) {
            long starttime = System.nanoTime();
            ThreadBuilder.get()
            .setShutdownHook(true)
            .setTask(() -> {
                long endtime = System.nanoTime();
                double runtime = ((double)(endtime - starttime)) / 1.0e9;
                System.out.format("received: %.2f/s\n", Stats.received.get() / runtime);
                System.out.format("dropped: %.2f/s\n", Stats.dropped.get() / runtime);
                System.out.format("sent: %.2f/s\n", Stats.sent.get() / runtime);
                System.out.format("failures: %.2f/s\n", Stats.processorFailures.get() / runtime);
                System.out.format("thrown: %.2f/s\n", Stats.thrown.get() / runtime);
            })
            .build();
        }

        try {
            if (configFile == null) {
                System.err.println("No configuration file given");
                exitcode = 1;
            } else {
                Properties props = Configuration.parse(configFile);
                if (sign) {
                    sign(signfile, props.jwtHandler);
                    exitcode = 0;
                } else if (!test) {
                    launch(props);
                    logger.warn("LogHub started");
                    exitcode = 0;
                } else if (testedprocessor != null) {
                    testProcessor(props, testedprocessor);
                }
            }
        } catch (ConfigException e) {
            String message = Helpers.resolveThrowableException(e);
            System.out.format("Error in %s: %s\n", e.getLocation(), message);
            exitcode = 1;
        } catch (IllegalStateException e) {
            exitcode = 1;
        } catch (RuntimeException e) {
            e.printStackTrace();
            exitcode = 1;
        } catch (IOException e) {
            System.out.format("can't read configuration file %s: %s\n", configFile, e.getMessage());
            exitcode = 11;
        }
        if (canexit && exitcode != 0) {
            System.exit(exitcode);
        } else if (exitcode != 0) {
            throw new RuntimeException();
        }
    }

    private void sign(String signfile, JWTHandler handler) {
        if (signfile == null) {
            System.err.println("No JWT payload");
        }
        try {
            byte[] buffer = Files.readAllBytes(Paths.get(signfile));
            String token = handler.sign(new String(buffer, StandardCharsets.UTF_8));
            System.out.println(token);
        } catch (IOException e) {
            System.err.println("Can't read JWT payload: " + Helpers.resolveThrowableException(e));
            logger.catching(e);
        }
    }

    private void testProcessor(Properties props, String testedprocessor2) {
        Processor p = props.identifiedProcessors.get(testedprocessor2);
        if (p == null) {
            System.err.println("Unidentified processor");
        } else if (! (p instanceof FieldsProcessor)) {
            System.err.println("Not a field processor");
        } else {
            p.configure(props);
            FieldsProcessor fp = (FieldsProcessor) p;
            Event ev = Event.emptyTestEvent(ConnectionContext.EMPTY);
            try {
                new BufferedReader(new InputStreamReader(System.in, "UTF-8")).lines().forEach( i -> {
                    try {
                        ev.put("message", i);
                        fp.fieldFunction(ev, i);
                        System.out.format("%s -> %s\n", i, ev);
                    } catch (ProcessorException e) {
                        System.err.println("Processing failed:" + e.getMessage());
                    }
                });
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    public void launch(Properties props) throws ConfigException, IOException {

        for (Source s: props.sources.values()) {
            if ( ! s.configure(props)) {
                logger.error("failed to start source {}", s.getName());
                throw new IllegalStateException();
            };
        }

        // Used to remember if configuration process succeded
        AtomicBoolean failed = new AtomicBoolean(false);

        props.pipelines.stream().forEach(i-> {
            boolean pipeOk = i.configure(props);
            failed.set(failed.get() || (! pipeOk));
        });

        failed.set(failed.get());

        for (Sender s: props.senders) {
            if (s.configure(props)) {
                s.start();
            } else {
                logger.error("failed to configure output {}", s.getName());
                failed.set(true);
            };
        }

        Set<EventsProcessor> allep = new HashSet<>(props.numWorkers);
        for (int i = 0; i < props.numWorkers; i++) {
            EventsProcessor t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
            t.start();
            allep.add(t);
        }
        Helpers.waitAllThreads(allep.stream().map(i -> (Thread) i ));

        for (Receiver r: props.receivers) {
            if (r.configure(props)) {
                r.start();
            } else {
                logger.error("failed to configure input {}", r.getName());
                failed.set(true);
            }
        }

        if (failed.get()) {
            throw new IllegalStateException();
        }

        Runnable shutdown = () -> {
            props.receivers.forEach( i -> i.stopReceiving());
            allep.forEach(i -> i.stopProcessing());
            props.senders.forEach( i -> i.stopSending());
            JmxService.stop();
        };
        shutdownAction = ThreadBuilder.get()
                        .setDaemon(false)
                        .setTask(shutdown)
                        .setName("StopEventsProcessors")
                        .setShutdownHook(true)
                        .build();

        try {
            JmxService.start(props.jmxServiceConfiguration);
        } catch (IOException e) {
            logger.error("JMX start failed: {}", Helpers.resolveThrowableException(e));
            shutdownAction.start();
            throw new IllegalStateException();
        }
        if (props.dashboardBuilder != null) {
            try {
                props.dashboardBuilder.build();
            } catch (IllegalArgumentException e) {
                logger.error("Unable to start HTTP dashboard: {}", Helpers.resolveThrowableException(e));
                shutdownAction.start();
                throw new IllegalStateException();
            } catch (InterruptedException e) {
                shutdownAction.start();
                Thread.currentThread().interrupt();
                throw new IllegalStateException();
            }
        }

    }

    public static void shutdown() {
        if (shutdownAction != null) {
            shutdownAction.run();
        }
    }

}
