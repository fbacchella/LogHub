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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.processors.FieldsProcessor;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.senders.Sender;

public class Start {
    
    /**
     * Used to define custom exit code, start at 10 because I don't know what exit code are reserver by the JVM.
     * For example, ExitOnOutOfMemoryError return 3.
     * 
     * @author Fabrice Bacchella
     *
     */
    private static class ExitCode {
        private static final int OK = 0;
        private static final int INVALIDCONFIGURATION = 10;
        public static final int FAILEDSTART = 11;
        public static final int FAILEDSTARTCRITICAL = 12;
    }

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
    int exitcode = ExitCode.OK;

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basis context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    private static final Logger logger = LogManager.getLogger();

    // It's exported for tests
    private static Thread shutdownAction;

    public static void main(final String[] args) {
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
            System.exit(ExitCode.OK);
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
        }
        if (dumpstats) {
            long starttime = System.nanoTime();
            ThreadBuilder.get()
            .setShutdownHook(true)
            .setTask(() -> {
                long endtime = System.nanoTime();
                double runtime = ((double)(endtime - starttime)) / 1.0e9;
                System.out.format("Received: %.2f/s%n", Stats.getReceived() / runtime);
                System.out.format("Dropped: %.2f/s%n", Stats.getDropped() / runtime);
                System.out.format("Sent: %.2f/s%\n", Stats.getSent() / runtime);
                System.out.format("Failures: %.2f/s%n", Stats.getFailed() / runtime);
                System.out.format("Exceptions: %.2f/s%n", Stats.getExceptionsCount() / runtime);
            })
            .build();
        }

        try {
            if (configFile == null) {
                System.err.println("No configuration file given");
                exitcode = ExitCode.INVALIDCONFIGURATION;
            } else {
                Properties props = Configuration.parse(configFile);
                if (sign) {
                    sign(signfile, props.jwtHandler);
                } else if (!test) {
                    launch(props);
                    logger.warn("LogHub started");
                } else if (testedprocessor != null) {
                    testProcessor(props, testedprocessor);
                }
            }
        } catch (ConfigException e) {
            String message = Helpers.resolveThrowableException(e);
            System.err.format("Error in %s: %s\n", e.getLocation(), message);
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IOException e) {
            System.err.format("can't read configuration file %s: %s\n", configFile, e.getMessage());
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IllegalStateException e) {
            // Thrown by launch when a component failed to start, details are in the logs
            System.err.format("Failed to start loghub: %s", e.getMessage());
            exitcode = ExitCode.FAILEDSTART;
        } catch (Throwable e) {
            System.err.format("Failed to start loghub for an unhandled cause: %s", e.getMessage());
            e.printStackTrace();
            exitcode = ExitCode.FAILEDSTARTCRITICAL;
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
        // Used to remember if configuration process succeded
        // So ensure that the whole configuration is tested instead needed
        // many tests
        boolean failed = false;

        for (Source s: props.sources.values()) {
            if (! s.configure(props)) {
                logger.error("failed to start source {}", s.getName());
                failed = true;
            };
        }

        Helpers.parallelStartProcessor(props);

        for (Sender s: props.senders) {
            try {
                if (s.configure(props)) {
                    s.start();
                } else {
                    logger.error("failed to configure sender {}", s.getName());
                    failed = true;
                };
            } catch (Throwable e) {
                if (Helpers.isFatal(e)) {
                    throw e;
                } else {
                    logger.error("failed to start sender {}", s.getClass().getName());
                    failed = true;
                }
            }
        }

        Set<EventsProcessor> allep = new HashSet<>(props.numWorkers);
        if (! failed) {
            for (int i = 0; i < props.numWorkers; i++) {
                EventsProcessor t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
                t.start();
                allep.add(t);
            }
            Helpers.waitAllThreads(allep.stream());
        }

        for (Receiver r: props.receivers) {
            try {
                if (r.configure(props)) {
                    r.start();
                } else {
                    logger.error("failed to configure receiver {}", r.getName());
                    failed = true;
                }
            } catch (Throwable e) {
                if (Helpers.isFatal(e)) {
                    throw e;
                } else {
                    logger.error("failed to start receiver {}", r.getClass().getName());
                    failed = true;
                }
            }
        }

        if (failed) {
            throw new IllegalStateException("Failed to start a component, see logs for more details");
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
            logger.catching(Level.DEBUG, e);
            shutdownAction.start();
            throw new IllegalStateException("JMX start failed: " + Helpers.resolveThrowableException(e));
        }
        if (props.dashboardBuilder != null) {
            try {
                props.dashboardBuilder.build();
            } catch (IllegalArgumentException e) {
                logger.error("Unable to start HTTP dashboard: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
                shutdownAction.start();
                throw new IllegalStateException("Unable to start HTTP dashboard: " + Helpers.resolveThrowableException(e));
            } catch (InterruptedException e) {
                shutdownAction.start();
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while starting dashboard");
            }
        }
    }

    public static void shutdown() {
        if (shutdownAction != null) {
            shutdownAction.run();
        }
    }

}
