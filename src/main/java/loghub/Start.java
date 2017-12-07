package loghub;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.NotBoundException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.codahale.metrics.JmxReporter;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.configuration.TestGrokPatterns;
import loghub.jmx.Helper;
import loghub.jmx.StatsMBean;
import loghub.jmx.PipelineStat;
import loghub.netty.http.AbstractHttpServer;

public class Start extends Thread {

    @Parameter(names = {"--configfile", "-c"}, description = "File")
    String configFile = null;
    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help;

    @Parameter(names = {"--test", "-t"}, description = "Test mode")
    boolean test = false;

    @Parameter(names = {"--fulltest", "-T"}, description = "Test mode")
    boolean fulltest = false;

    @Parameter(names = {"--stats", "-s"}, description = "Dump stats on exit")
    boolean dumpstats = false;

    @Parameter(names = "--canexit", description = "Prevent call to System.exit(), for JUnit tests only", hidden = true)
    boolean canexit = true;

    String grokPatterns = null;
    String pipeLineTest = null;
    int exitcode = 0;

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basis context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
    }

    private static final Logger logger = LogManager.getLogger();

    static public void main(final String[] args) {
        Start main = new Start();
        JCommander jcom = JCommander
                .newBuilder()
                .addObject(main)
                .build();

        // A null in the argument, so it was called from inside a jvm, never exit
        main.canexit = false;
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

        if (grokPatterns != null) {
            TestGrokPatterns.check(grokPatterns);
            exitcode = 0;
        } else if (pipeLineTest != null) {
            TestEventProcessing.check(pipeLineTest, configFile);
            exitcode = 0;
        }
        if (dumpstats) {
            final long starttime = System.nanoTime();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public synchronized void start() {
                    try {
                        long endtime = System.nanoTime();
                        double runtime = ((double)(endtime - starttime)) / 1.0e9;
                        System.out.format("received: %.2f/s\n", Stats.received.get() / runtime);
                        System.out.format("dropped: %.2f/s\n", Stats.dropped.get() / runtime);
                        System.out.format("sent: %.2f/s\n", Stats.sent.get() / runtime);
                        System.out.format("failed: %.2f/s\n", Stats.failed.get() / runtime);
                        System.out.format("thrown: %.2f/s\n", Stats.thrown.get() / runtime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        try {
            Properties props = Configuration.parse(configFile);
            if (!test) {
                if (!fulltest) {
                    launch(props);
                    logger.warn("LogHub started");
                    exitcode = 0;
                }
            }
        } catch (ConfigException e) {
            Throwable t = e;
            if (e.getCause() != null) {
                t = e.getCause();
            }
            String message = t.getMessage();
            if (message == null) {
                message = t.getClass().getSimpleName();
            }
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

    public void launch(Properties props) throws ConfigException, IOException {

        setName("LogHub");

        for (Source s: props.sources.values()) {
            if ( ! s.configure(props)) {
                logger.error("failed to start output {}", s.getName());
                throw new IllegalStateException();
            };
        }

        props.pipelines.stream().forEach(i-> i.configure(props));

        for (Sender s: props.senders) {
            if (s.configure(props)) {
                s.start();
            } else {
                logger.error("failed to start output {}", s.getName());
                throw new IllegalStateException();
            };
        }

        for (int i = 0; i < props.numWorkers; i++) {
            Thread t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
            t.setName("ProcessingThread" + i);
            t.setDaemon(true);
            t.start();
        }

        for (Receiver r: props.receivers) {
            if (r.configure(props)) {
                r.start();
            } else {
                logger.error("failed to start input {}", r.getName());
                throw new IllegalStateException();
            }
        }

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new StatsMBean.Implementation(), StatsMBean.Implementation.NAME);
            JmxReporter reporter = Properties.metrics.getJmxReporter();
            reporter.start();
            mbs.queryNames(ObjectName.getInstance("metrics", "name", "Pipeline.*.timer"), null).stream()
            .map( i-> i.getKeyProperty("name"))
            .map( i -> i.replaceAll("^Pipeline\\.(.*)\\.timer$", "$1"))
            .forEach(
                    i -> {
                        try {
                            mbs.registerMBean(new PipelineStat.Implementation(i), null);
                        } catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
                        }
                    })
            ;
            int port = props.jmxport;
            if (port > 0) {
                Helper.start(props.jmxproto, props.jmxlisten, port);
            }
        } catch (IOException | NotBoundException | NotCompliantMBeanException | MalformedObjectNameException
                | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            throw new RuntimeException("jmx configuration failed: " + e.getMessage(), e);
        }

        if (props.httpPort >= 0) {
            AbstractHttpServer server = new DashboardHttpServer();
            server.setPort(props.httpPort);
            server.configure(props);
        }

    }

    public void run() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
