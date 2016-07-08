package loghub;

import java.io.IOException;
import java.rmi.NotBoundException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.remote.JMXConnectorServer;

import loghub.configuration.Configuration;
import loghub.jmx.Helper;

public class Start extends Thread {

    static public void main(final String[] args) {
        try {
            new Start(args[0]).start();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        }
    }

    public Start(String configFile) {

        setName("LogHub");

        Configuration conf = new Configuration();

        conf.parse(configFile);

        conf.pipelines.stream().forEach(i-> i.configure(conf.properties));

        for(Sender s: conf.getSenders()) {
            if(s.configure(conf.properties)) {
                s.start();
            };
        }

        for(Receiver r: conf.getReceivers()) {
            if(r.configure(conf.properties)) {
                r.start();
            }
        }

        for(int i = 0; i < conf.properties.numWorkers; i++) {
            Thread t = new EventsProcessor(conf.properties.mainQueue, conf.properties.outputQueues);
            t.setName("ProcessingThread" + i);
            t.setDaemon(true);
            t.start();
        }

        try {
            int port = conf.properties.jmxport;
            if(port > 0) {
                @SuppressWarnings("unused")
                JMXConnectorServer cs = Helper.start(conf.properties.jmxproto, conf.properties.jmxlisten, port);
                Helper.register(loghub.jmx.Stats.class);
            }
        } catch (IOException | NotBoundException | NotCompliantMBeanException | MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("jmx configuration failed: " + e.getMessage(), e);
        }

    }

    public void run() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
        }
        SmartContext.terminate();
    }

}
