package loghub.jmx;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class TestJmxConnection {

    private JMXConnectorServer server = null;

    private static final String hostip;
    static {
        try {
            hostip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException ex) {
            throw new UncheckedIOException(ex);
        }
    }
    private static final String loopbackip = InetAddress.getLoopbackAddress().getHostAddress();
    private final int port = Tools.tryGetPort();

    static {
        Map<String, String> env = new HashMap<>();

        env.put("sun.rmi.transport.tcp.readTimeout", "500");
        env.put("sun.rmi.transport.connectionTimeout", "500");
        env.put("sun.rmi.transport.proxy.connectTimeout", "500");
        env.put("java.rmi.server.disableHttp", "true");
        env.put("sun.rmi.transport.tcp.handshakeTimeout", "500");
        env.put("sun.rmi.transport.tcp.responseTimeout", "500");

        System.getProperties().putAll(env);
    }

    @After
    public void stopJmx() throws IOException {
        if (server != null) {
            server.stop();
        }
        Properties.metrics.stopJmxReporter();
    }

    @Test
    public void loadConf1() throws Exception {
        String configStr = "jmx.port: " +  port + "\njmx.hostname: \"" + hostip +"\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        MBeanServer mbs = MBeanServerFactory.newMBeanServer();
        server = props.jmxBuilder.setMbs(mbs).start();
        connect(hostip, loopbackip);
    }

    @Test
    public void loadConf2() throws Exception {
        String configStr = "jmx.port: " +  port + "\njmx.hostname: \"" + loopbackip +"\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        MBeanServer mbs = MBeanServerFactory.newMBeanServer();
        server = props.jmxBuilder.setMbs(mbs).start();
        connect(loopbackip, hostip);
    }

    private void connect(String ip1, String ip2) throws IOException, InstanceNotFoundException {
        JMXServiceURL url = 
                        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + loopbackip + ":" + port + "/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url, Collections.emptyMap());
        String cnxId = jmxc.getConnectionId();
        Assert.assertTrue(cnxId.contains(ip1));
        Assert.assertFalse(cnxId.contains(ip2));
        Assert.assertNotNull(jmxc.getMBeanServerConnection().getObjectInstance(ExceptionsMBean.Implementation.NAME));
        Assert.assertNotNull(jmxc.getMBeanServerConnection().getObjectInstance(StatsMBean.Implementation.NAME));
    }

}
