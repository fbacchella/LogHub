package loghub.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.Parameter;

import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.ShutdownTask;
import loghub.SystemdHandler;
import loghub.ThreadBuilder;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.events.Event;
import loghub.metrics.JmxService;
import loghub.processors.FieldsProcessor;
import loghub.receivers.Receiver;
import loghub.senders.Sender;
import loghub.sources.Source;

public class Launch implements BaseCommand {

    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--configfile", "-c"}, description = "File")
    private String configFile = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help;

    @Parameter(names = {"--test", "-t"}, description = "Test mode")
    private boolean test = false;

    @Parameter(names = {"--stats", "-s"}, description = "Dump stats on exit")
    private boolean dumpstats = false;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--testprocessor", "-p"}, description = "A field processor to test")
    private String testedprocessor = null;

    String pipeLineTest = null;

    @Override
    public int run(List<String> unknownOptions) {
        int exitcode = ExitCode.DONTEXIT;
        if (testedprocessor != null) {
            test = true;
            dumpstats = false;
        }
        if (pipeLineTest != null) {
            TestEventProcessing.check(pipeLineTest, configFile);
        }

        try {
            if (configFile == null) {
                System.err.println("No configuration file given");
                exitcode = ExitCode.INVALIDCONFIGURATION;
            } else {
                SystemdHandler systemd = SystemdHandler.start();
                systemd.setStatus("Parsing configuration");
                Properties props = Configuration.parse(configFile);
                if (!test) {
                    systemd.setStatus("Launching");
                    launch(props, systemd);
                    systemd.setStatus("Started");
                    systemd.started();
                    logger.warn("LogHub started");
                } else if (testedprocessor != null) {
                    testProcessor(props, testedprocessor);
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
        } catch (Throwable e) {
            System.err.format("Failed to start loghub for an unhandled cause: %s%n", Helpers.resolveThrowableException(e));
            e.printStackTrace();
            exitcode = ExitCode.FAILEDSTARTCRITICAL;
        }
        return exitcode;
    }

    @Override
    public <T> Optional<T> getField(String name, Class<T> tClass) {
        switch (name) {
        case "configFile":
            return Optional.of((T) configFile);
        case "help":
            return Optional.of((T) Boolean.valueOf(help));
        default:
            return Optional.empty();
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
            Event ev = props.eventsFactory.newEvent();
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).lines().forEach(i -> {
                try {
                    ev.put("message", i);
                    fp.fieldFunction(ev, i);
                    System.out.format("%s -> %s%n", i, ev);
                } catch (ProcessorException e) {
                    System.err.println("Processing failed:" + e.getMessage());
                }
            });
        }
    }

    public void launch(Properties props, SystemdHandler systemd) throws ConfigException {
        try {
            JmxService.start(props.jmxServiceConfiguration);
        } catch (IOException e) {
            logger.error("JMX start failed: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            throw new IllegalStateException("JMX start failed: " + Helpers.resolveThrowableException(e));
        }

        // Used to remember if configuration process succeded
        // So ensure that the whole configuration is tested instead needed
        // many tests
        boolean failed = false;

        long starttime = System.nanoTime();

        for (Map.Entry<String, Source> s : props.sources.entrySet()) {
            if (! s.getValue().configure(props)) {
                logger.error("failed to start source {}", s.getKey());
                failed = true;
            }
        }

        Helpers.parallelStartProcessor(props);

        for (Sender s : props.senders) {
            s.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            try {
                if (s.configure(props)) {
                    s.start();
                } else {
                    logger.error("failed to configure sender {}", s.getName());
                    failed = true;
                }
            } catch (Throwable e) {
                if (Helpers.isFatal(e)) {
                    throw e;
                } else {
                    logger.error("failed to start sender {}", s.getClass().getName());
                    failed = true;
                }
            }
        }

        if (! failed) {
            for (EventsProcessor ep : props.eventsprocessors) {
                ep.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
                ep.start();
            }
            Helpers.waitAllThreads(props.eventsprocessors.stream());
        }

        for (Receiver<?, ?> r : props.receivers) {
            r.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            try {
                if (r.configure(props)) {
                    // Only start if not failed. Avoid swallowing events and latter discard them
                    if (! failed) {
                        r.start();
                    }
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

        // The dashboard is used as an isAlive, last things to start.
        if (props.dashboard != null) {
            try {
                props.dashboard.start();
            } catch (IllegalArgumentException e) {
                logger.error("Unable to start HTTP dashboard: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
                throw new IllegalStateException("Unable to start HTTP dashboard: " + Helpers.resolveThrowableException(e));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while starting dashboard");
            }
        }

        // The shutdown runnable needs to be able to run on degraded JVM,
        // so reduced allocation and executed code inside
        Receiver<?, ?>[] receivers = props.receivers.toArray(Receiver[]::new);
        EventsProcessor[] eventProcessors = props.eventsprocessors.toArray(EventsProcessor[]::new);
        Sender[] senders = props.senders.toArray(Sender[]::new);

        ShutdownTask.configuration()
                    .eventProcessors(eventProcessors)
                    .loghubtimer(props.timer)
                    .startTime(starttime)
                    .repositories(props.eventsRepositories())
                    .dumpStats(dumpstats)
                    .eventProcessors(eventProcessors)
                    .systemd(systemd)
                    .receivers(receivers)
                    .senders(senders)
                    .terminator(props.terminator())
                    .hprofDumpPath(props.hprofdump)
                    .asShutdownHook(true)
                    .register();
    }

}
