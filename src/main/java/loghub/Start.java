package loghub;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.remote.JMXConnectorServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.JmxReporter;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.TestEventProcessing;
import loghub.configuration.TestGrokPatterns;
import loghub.jmx.Helper;
import loghub.netty.http.AbstractHttpServer;

public class Start extends Thread {

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basis context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
    }

    private static final Logger logger = LogManager.getLogger();

    static public void main(final String[] args) {

        String configFile = null;
        boolean test = false;
        boolean fulltest = false;
        String grokPatterns = null;
        String pipeLineTest = null;
        boolean canexit = true;
        int exitcode = 0;

        if (args.length > 0) {
            List<String> argsList = Arrays.asList(args);
            Iterator<String> i = argsList.iterator();
            while (i.hasNext()) {
                String arg = i.next();
                if ("-c".equals(arg) || "--config".equals(arg)) {
                    if (i.hasNext()) {
                        configFile = i.next();
                    }
                } else if ("-t".equals(arg) || "--test".equals(arg)) {
                    test = true;
                } else if ("-T".equals(arg) || "--fulltest".equals(arg)) {
                    fulltest = true;
                } else if ("-g".equals(arg) || "--grok".equals(arg)) {
                    grokPatterns = i.next();
                } else if ("-p".equals(arg) || "--pipeline".equals(arg)) {
                    pipeLineTest = i.next();
                } else if (arg == null) {
                    // A null in the argument, so it was called from inside a jvm, never exit
                    canexit = false;
                } else {
                    configFile = arg;
                }
            }
        }

        if (grokPatterns != null) {
            TestGrokPatterns.check(grokPatterns);
            exitcode = 0;
        } else if (pipeLineTest != null) {
            TestEventProcessing.check(pipeLineTest, configFile);
            exitcode = 0;
        }

        try {
            Properties props = Configuration.parse(configFile);
            if (!test) {
                Start runner = new Start(props);
                if (!fulltest) {
                    runner.start();
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

    public Start(Properties props) throws ConfigException, IOException {

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
            Helper.register(loghub.jmx.Stats.class);
            JmxReporter reporter = Properties.metrics.getJmxReporter();
            reporter.start();
            int port = props.jmxport;
            if (port > 0) {
                @SuppressWarnings("unused")
                JMXConnectorServer cs = Helper.start(props.jmxproto, props.jmxlisten, port);
            }
        } catch (IOException | NotBoundException | NotCompliantMBeanException | MalformedObjectNameException
                | InstanceAlreadyExistsException | MBeanRegistrationException | InstantiationException
                | IllegalAccessException e) {
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
