package loghub.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.NotBoundException;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

public class Helper {

    static final public PROTOCOL DEFAULTPROTOCOL = PROTOCOL.rmi;
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
        Map<String, Object> jmxenv = new HashMap<>();
        if (protocol == PROTOCOL.rmi) {
            // If listen bound to a given ip, and jmx protocol is rmi, use it as the rmi's property hostname.
            if (! "0.0.0.0".equals(host) && System.getProperty("java.rmi.server.hostname") == null) {
                System.setProperty("java.rmi.server.hostname", host);
                // Not sure if it's useful, hostname is resolve in sun.rmi.transport.tcp.TCPEndpoint, using InetAddress.getLocalHost() if 
                // this property is not defined.
                // But it might be hiding in other places
                jmxenv.put("java.rmi.server.hostname", host);
            }
            protocolString = "rmi";
            java.rmi.registry.LocateRegistry.createRegistry(port);
            path = "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
        }
        url = new JMXServiceURL(protocolString, host, port, path);
        mbs = ManagementFactory.getPlatformMBeanServer();
        cs = JMXConnectorServerFactory.newJMXConnectorServer(url, jmxenv, mbs);
        cs.start();
        return cs;
    }

    private Helper() {};

}
