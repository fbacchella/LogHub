package loghub.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.NotBoundException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import loghub.Helpers;
import loghub.configuration.BeansManager;

public class Helper {

    static final public PROTOCOL defaultProto = PROTOCOL.rmi;
    public static enum PROTOCOL {
        rmi,
        jmxmp,
    }

    public static JMXConnectorServer start(PROTOCOL protocol, String host, int port) throws IOException, NotBoundException {
        MBeanServer mbs;
        JMXServiceURL url;
        JMXConnectorServer cs;

        String path = "/";
        String protocolString = protocol.toString();
        if (protocol == PROTOCOL.rmi) {
            protocolString = "rmi";
            java.rmi.registry.LocateRegistry.createRegistry(port);
            path = "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
        }
        url = new JMXServiceURL(protocolString, host, port, path);
        mbs = ManagementFactory.getPlatformMBeanServer();
        cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
        cs.start();
        JMXServiceURL addr = cs.getAddress();
        JMXConnectorFactory.connect(addr);
        return cs;
    }

    public static void register(Class<?>... beans) throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, InstantiationException, IllegalAccessException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        Helpers.ThrowingFunction<Implementation, BeanImplementation> instanciate = i-> i.value().newInstance();
        Helpers.ThrowingConsumer<BeanImplementation> register = i -> mbs.registerMBean(i, new ObjectName(i.getName()));

        for(Class<?> bean: beans) {
            BeansManager.enumerateAnnotation(bean, Implementation.class, Object.class).stream()
            .findFirst().map(instanciate)
            .ifPresent(register);
        }
    }

    private Helper() {};

}
