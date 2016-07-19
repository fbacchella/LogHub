package loghub;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.remote.JMXConnectorServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.jmx.Helper;
import loghub.netty.http.HttpServer;

public class Start extends Thread {

    private static final Logger logger = LogManager.getLogger();

    static public void main(final String[] args) {
        try {
            new Start(args[0]).start();
        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public Start(String configFile) {

        setName("LogHub");

        Properties props = Configuration.parse(configFile);

        props.pipelines.stream().forEach(i-> i.configure(props));

        for (Sender s: props.senders) {
            if (s.configure(props)) {
                s.start();
            } else {
                logger.error("failed to start output {}", s.getName());
            };
        }

        for (Receiver r: props.receivers) {
            if (r.configure(props)) {
                r.start();
            } else {
                logger.error("failed to start input {}", r.getName());
            }
        }

        for (int i = 0; i < props.numWorkers; i++) {
            Thread t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine);
            t.setName("ProcessingThread" + i);
            t.setDaemon(true);
            t.start();
        }

        try {
            int port = props.jmxport;
            if (port > 0) {
                @SuppressWarnings("unused")
                JMXConnectorServer cs = Helper.start(props.jmxproto, props.jmxlisten, port);
            }
            Helper.register(loghub.jmx.Stats.class);
        } catch (IOException | NotBoundException | NotCompliantMBeanException | MalformedObjectNameException
                | InstanceAlreadyExistsException | MBeanRegistrationException | InstantiationException
                | IllegalAccessException e) {
            throw new RuntimeException("jmx configuration failed: " + e.getMessage(), e);
        }
        
        if (props.httpPort >= 0) {
            HttpServer server = new HttpServer();
            server.setIpAddr(new InetSocketAddress((InetAddress) null, props.httpPort));
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
