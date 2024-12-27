package loghub.metrics;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.SSLContext;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class TestJmxConnection {

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
    private SSLContext defaultSslContext;

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

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Before
    public void saveSslContext() throws NoSuchAlgorithmException {
        defaultSslContext = SSLContext.getDefault();
    }

    @After
    public void stopJmx() {
        JmxService.stop();
        SSLContext.setDefault(defaultSslContext);
    }

    @Test
    public void loadConf1() throws Exception {
        String configStr = "jmx.port: " +  port + "\njmx.hostname: \"" + hostip +"\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        JmxService.start(props.jmxServiceConfiguration);
        connect(hostip, loopbackip);
    }

    @Test
    public void loadConf2() throws Exception {
        String configStr = "jmx.port: " +  port + "\njmx.hostname: \"" + loopbackip +"\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        JmxService.start(props.jmxServiceConfiguration);
        connect(loopbackip, hostip);
    }

    @Test
    public void loadConfLoopback() throws Exception {
        String configStr = "jmx.port: " +  port + "\njmx.listen: \"127.0.0.1\" jmx.hostname: \"127.0.0.1\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        JmxService.start(props.jmxServiceConfiguration);
        connect(loopbackip, hostip);
    }

    @Test
    public void loadUrl() throws Exception {
        String jmxUrl = String.format("service:jmx:rmi://127.0.0.1:%d/jndi/rmi://127.0.0.1:%d/jmxrmi", port, port);
        String configStr = "jmx.hostname: \"127.0.0.1\" jmx.serviceUrl: \"" +  jmxUrl + "\"";
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        JmxService.start(props.jmxServiceConfiguration);
        connect(loopbackip, hostip);
    }

    @Test
    public void loadSsl() throws Exception {
        String sslParams = "{cipherSuites: [\"TLS_AES_256_GCM_SHA384\"], protocols: [\"TLSv1.3\"]}";
        String p12store = Tools.class.getResource("/loghub.p12").getFile();
        String sslContext = String.format("{name: \"TLSv1.3\", trusts: [\"%s\"]}", p12store);
        String jmxUrl = String.format("service:jmx:rmi://127.0.0.1:%d/jndi/rmi://127.0.0.1:%d/jmxrmi", port, port);
        String configStr = String.format("jmx.withSsl: true jmx.hostname: \"127.0.0.1\" jmx.serviceUrl: \"%s\" jmx.sslContext: %s jmx.sslParams: %s ssl.trusts: [\"%s\"] ssl.context: \"TLSv1.3\"",
                jmxUrl, sslContext, sslParams, p12store);
        StringReader reader = new StringReader(configStr);
        Properties props = Configuration.parse(reader);
        SSLContext.setDefault(props.ssl);
        JmxService.start(props.jmxServiceConfiguration);
        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());

        connect(loopbackip, hostip, env);
    }

    private void connect(String ip1, String ip2) throws IOException, InstanceNotFoundException {
        connect(ip1, ip2, Map.of());
    }

    private void connect(String ip1, String ip2, Map<String, Object> env) throws IOException, InstanceNotFoundException {
        String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",loopbackip, port);
        JMXServiceURL url = new JMXServiceURL(jmxUrl);
        try (JMXConnector jmxc = JMXConnectorFactory.connect(url, env)) {
            String cnxId = jmxc.getConnectionId();
            Assert.assertTrue(cnxId.contains(ip1));
            Assert.assertFalse(cnxId.contains(ip2));
            Assert.assertNotNull(jmxc.getMBeanServerConnection().getObjectInstance(ExceptionsMBean.Implementation.NAME));
            Assert.assertNotNull(jmxc.getMBeanServerConnection().getObjectInstance(StatsMBean.Implementation.NAME));
        }
    }

}
