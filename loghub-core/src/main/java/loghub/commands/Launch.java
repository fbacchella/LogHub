package loghub.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.Parameter;

import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.ShutdownTask;
import loghub.SystemdHandler;
import loghub.ThreadBuilder;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.metrics.JmxService;
import loghub.receivers.Receiver;
import loghub.senders.Sender;
import loghub.sources.Source;

public class Launch implements BaseParametersRunner {

    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--configfile", "-c"}, description = "File")
    private String configFile = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--test", "-t"}, description = "Test mode")
    private boolean test = false;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--stats", "-s"}, description = "Dump stats on exit")
    private boolean dumpstats = false;

    @Override
    public void reset() {
        configFile = null;
        test = false;
        dumpstats = false;
    }

    @Override
    public int run(List<String> mainParameters, PrintWriter out, PrintWriter err) {
        int exitcode;
        try {
            if (configFile == null) {
                err.println("No configuration file given");
                exitcode = ExitCode.INVALIDCONFIGURATION;
            } else if (test){
                Configuration.parse(configFile);
                exitcode = ExitCode.OK;
            } else {
                SystemdHandler systemd = SystemdHandler.start();
                systemd.setStatus("Parsing configuration");
                Properties props = Configuration.parse(configFile);
                systemd.setStatus("Launching");
                launch(props, systemd, out, err);
                systemd.setStatus("Started");
                systemd.started();
                logger.warn("LogHub started");
                exitcode = ExitCode.DONTEXIT;
            }
        } catch (ConfigException e) {
            err.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IOException e) {
            err.format("Can't read configuration file %s: %s%n", configFile, Helpers.resolveThrowableException(e));
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IllegalStateException e) {
            // Thrown by launch when a component failed to start, details are in the logs
            err.format("Failed to start loghub: %s%n", Helpers.resolveThrowableException(e));
            exitcode = ExitCode.FAILEDSTART;
        } catch (Throwable e) {
            err.format("Failed to start loghub for an unhandled cause: %s%n", Helpers.resolveThrowableException(e));
            e.printStackTrace(err);
            exitcode = ExitCode.FAILEDSTARTCRITICAL;
        }
        return exitcode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getField(String name) {
        if (name.equals("configFile")) {
            return Optional.ofNullable((T) configFile);
        } else {
            return Optional.empty();
        }
    }

    public void launch(Properties props, SystemdHandler systemd, PrintWriter out, PrintWriter err) throws ConfigException {
        Thread.currentThread().setContextClassLoader(props.classloader);

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
            s.setContextClassLoader(props.classloader);
            try {
                if (s.configure(props)) {
                    s.start();
                } else {
                    logger.error("failed to configure sender {}", s.getName());
                    failed = true;
                }
            } catch (Exception e) {
                logger.atError().withThrowable(e).log("failed to start sender {}: {}", () -> s.getClass().getName(), () -> Helpers.resolveThrowableException(e));
                failed = true;
            }
        }

        if (! failed) {
            for (EventsProcessor ep : props.eventsprocessors) {
                ep.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
                ep.setContextClassLoader(props.classloader);
                ep.start();
            }
            Helpers.waitAllThreads(props.eventsprocessors.stream());
        }

        for (Receiver<?, ?> r : props.receivers) {
            r.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            r.setContextClassLoader(props.classloader);
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

        try {
            JmxService.start(props.jmxServiceConfiguration);
        } catch (IOException e) {
            logger.error("JMX start failed: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            throw new IllegalStateException("JMX start failed: " + Helpers.resolveThrowableException(e));
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
                    .out(out)
                    .err(err)
                    .register();
    }

}
