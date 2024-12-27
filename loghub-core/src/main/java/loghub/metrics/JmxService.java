package loghub.metrics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.security.auth.Subject;

import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;

import loghub.Helpers;
import loghub.Pipeline;
import loghub.receivers.Receiver;
import loghub.security.AuthenticationHandler;
import loghub.senders.Sender;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class JmxService {

    public static final String DEFAULTPROTOCOL = "rmi";

    public static Configuration configuration() {
        return new Configuration();
    }

    @Accessors(chain = true)
    public static class Configuration {
        @Setter
        private String protocol = DEFAULTPROTOCOL;
        @Setter
        private String protocolProvider = null;
        @Setter
        private int port = -1;
        @Setter
        private String hostname = null;
        // Mainly used for tests
        @Setter
        private SSLContext sslContext = null;
        @Setter
        private SSLParameters sslParams = null;
        @Setter
        private boolean withSsl = false;
        @Setter
        private String jaasName = null;
        @Setter
        private javax.security.auth.login.Configuration jaasConfig = null;
        @Setter
        private ClassLoader classloader = Configuration.class.getClassLoader();
        @Setter
        private JMXServiceURL serviceUrl = null;
        @Setter
        private InetAddress listen;

        private final Map<ObjectName, Object> mbeans = new HashMap<>();

        private Configuration() {
            try {
                listen = InetAddress.getByName("0.0.0.0");
            } catch (UnknownHostException e) {
                throw new IllegalStateException(e);
            }
        }

        public Configuration setProperties(Map<String, Object> values) {
            values.forEach(this::setProperty);
            return this;
        }

        public Configuration setProperty(String key, Object value) {
            switch(key) {
            case "protocol": 
                protocol = value.toString().toLowerCase(Locale.ENGLISH);
                break;
            case "providerPackages":
                protocolProvider = value.toString().toLowerCase(Locale.ENGLISH);
                break;
            case "port":
                port = ((Number)value).intValue();
                break;
            case "hostname": 
                hostname = value.toString();
                break;
            case "jaasName": 
                jaasName = value.toString();
                break;
            case "withSsl": 
                withSsl = (Boolean) value;
                break;
            case "sslContext":
                sslContext = (SSLContext) value;
                break;
            case "sslParams":
                sslParams = (SSLParameters) value;
                break;
            case "classLoader":
                classloader = (ClassLoader) value;
                break;
            case "listen":
                try {
                    listen = InetAddress.getByName(value.toString());
                } catch (UnknownHostException ex) {
                    throw new IllegalArgumentException("Invalid listen address for JMX service URL: " + Helpers.resolveThrowableException(ex), ex);
                }
                break;
            case "serviceUrl":
                try {
                    serviceUrl = new JMXServiceURL(value.toString());
                } catch (MalformedURLException ex) {
                    throw new IllegalArgumentException("Invalid JMX service URL: " + Helpers.resolveThrowableException(ex), ex);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown property " + key);
            }
            return this;
        }

        public Configuration register(ObjectName key, Object value) {
            mbeans.put(key, value);
            return this;
        }

        public Configuration registerReceivers(Collection<Receiver<?, ?>> receivers) {
            receivers.forEach(r -> {
                try {
                    ReceiverMBean.Implementation rmb = new ReceiverMBean.Implementation(r);
                    mbeans.put(rmb.getObjectName(), rmb);
                } catch (NotCompliantMBeanException
                                | MalformedObjectNameException e) {
                    // Ignored exception
                }
            });
            try {
                ReceiverMBean.Implementation rmb = new ReceiverMBean.Implementation(null);
                mbeans.put(rmb.getObjectName(), rmb);
            } catch (NotCompliantMBeanException
                            | MalformedObjectNameException e) {
                // Ignored exception
            }
            return this;
        }

        public Configuration registerSenders(Collection<Sender> senders) {
            senders.forEach(s -> {
                try {
                    SenderMBean.Implementation smb = new SenderMBean.Implementation(s);
                    mbeans.put(smb.getObjectName(), smb);
                } catch (NotCompliantMBeanException
                                | MalformedObjectNameException e) {
                    // Ignored exception
                }
            });
            try {
                SenderMBean.Implementation smb = new SenderMBean.Implementation(null);
                mbeans.put(smb.getObjectName(), smb);
            } catch (NotCompliantMBeanException
                            | MalformedObjectNameException e) {
                // Ignored exception
            }
            return this;
        }

        public Configuration registerPipelines(Collection<Pipeline> pipelines) {
            pipelines.forEach(p -> {
                PipelineMBean.Implementation pmb;
                try {
                    pmb = new PipelineMBean.Implementation(p.getName());
                    mbeans.put(pmb.getObjectName(), pmb);
                } catch (NotCompliantMBeanException
                                | MalformedObjectNameException e) {
                    // Ignored exception
                }
            });
            try {
                PipelineMBean.Implementation pmb = new PipelineMBean.Implementation(null);
                mbeans.put(pmb.getObjectName(), pmb);
            } catch (NotCompliantMBeanException | MalformedObjectNameException e) {
                // Ignored exception
            }

            return this;
        }
    }

    private static JmxReporter reporter;
    private static MBeanServer mbs;
    private static JMXConnectorServer server;
    private static final Set<ObjectName> registered = new HashSet<>();

    public static void start(Configuration conf) throws IOException {
        mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            conf.mbeans.forEach((k,v) -> {
                try {
                    mbs.registerMBean(v,k);
                    registered.add(k);
                } catch (InstanceAlreadyExistsException
                                | MBeanRegistrationException
                                | NotCompliantMBeanException ex) {
                    throw new UndeclaredThrowableException(ex);
                }
            });
        } catch (UndeclaredThrowableException ex) {
            throw new IllegalStateException("Unusuable JMX setup: " + Helpers.resolveThrowableException(ex.getCause()), ex);
        }

        startJmxReporter();

        if (conf.port >= 0 || conf.serviceUrl !=  null) {
            server = startConnectorServer(conf);
        }
    }

    private static ObjectName createMetricName(String type, String domain, String name, ObjectNameFactory donf, Pattern pipepattern ) {
        ObjectName metricName;
        Matcher m = pipepattern.matcher(name);
        if (m.matches()) {
            Map<String, String> table = getStringStringHashtable(m);
            try {
                metricName = new ObjectName("loghub", new Hashtable<>(table));
            } catch (MalformedObjectNameException e) {
                metricName = donf.createName(type, domain, name);
            }
        } else {
            metricName = donf.createName(type, domain, name);
        }
        return metricName;
    }

    private static Map<String, String> getStringStringHashtable(Matcher m) {
        Map<String, String> table = new HashMap<>(4);
        String service = m.group(1);
        table.put("type", service);
        if (m.group(3) != null) {
            String servicename = m.group(2);
            String metric = m.group(4);
            table.put("servicename", servicename);
            table.put("name", metric);
        } else {
            String metric = m.group(2);
            table.put("name", metric);
            table.put("level", "details");
        }
        return table;
    }

    private static void startJmxReporter() {
        ObjectNameFactory donf = new DefaultObjectNameFactory();
        Pattern pipepattern = Pattern.compile("^([^.]+)\\.(.+?)(\\.([a-zA-Z0-9]+))?$");
        ObjectNameFactory factory = (t, d, n) -> createMetricName(t, d, n, donf, pipepattern);
        reporter = JmxReporter.forRegistry(Stats.metricsRegistry).createsObjectNamesWith(factory).registerWith(mbs).build();
        reporter.start();
    }

    private static JMXConnectorServer startConnectorServer(Configuration conf) throws IOException {
        Map<String, Object> env = new HashMap<>();
        env.put(JMXConnectorServerFactory.DEFAULT_CLASS_LOADER, conf.classloader);
        env.put(JMXConnectorServerFactory.PROTOCOL_PROVIDER_CLASS_LOADER, conf.classloader);
        RMIClientSocketFactory csf = null;
        RMIServerSocketFactory ssf = null;
        if (conf.hostname != null) {
            System.setProperty("java.rmi.server.hostname", conf.hostname);
            System.setProperty("java.rmi.server.useLocalHostname", "false");
        }
        if (conf.withSsl) {
            if (conf.sslParams == null) {
                conf.sslParams = conf.sslContext.getDefaultSSLParameters();
            }
            csf = new SslRMIClientSocketFactory();
            ssf = getServerSocketFactory(conf);
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);
            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, csf);
            env.put("com.sun.jndi.rmi.factory.socket", csf);
        }
        if (conf.jaasName != null) {
            AuthenticationHandler ah = AuthenticationHandler.getBuilder()
                            .setJaasName(conf.jaasName)
                            .setJaasConfig(conf.jaasConfig)
                            .build();

            env.put(JMXConnectorServer.AUTHENTICATOR, (JMXAuthenticator) credentials -> {
                Principal p = null;
                if ((credentials instanceof String[])) {
                    String[] loginPassword = (String[]) credentials;
                    if (loginPassword.length == 2) {
                        p = ah.checkLoginPassword(loginPassword[0],
                                                  loginPassword[1].toCharArray());
                        loginPassword[1] = null;
                        if (p == null) {
                            throw new SecurityException("Invalid user");
                        }
                    }
                }
                if (p == null) {
                    throw new SecurityException("Invalid configuration");
                } else {
                    return new Subject(true,
                                       Collections.singleton(p),
                                       Collections.emptySet(),
                                       Collections.emptySet());
                }
            });
        }
        if (conf.protocolProvider != null) {
            env.put(JMXConnectorServerFactory.PROTOCOL_PROVIDER_PACKAGES, conf.protocolProvider);
        }
        JMXServiceURL listen;
        if (conf.serviceUrl != null) {
            listen = conf.serviceUrl;
        } else {
            String path = "/";
            if ("rmi".equals(conf.protocol)) {
                path = String.format("/jndi/rmi://%s:%d/jmxrmi", conf.listen.getHostAddress(), conf.port);
            }
            listen = new JMXServiceURL(conf.protocol, conf.listen.getHostAddress(), conf.port, path);
        }
        if ("rmi".equals(listen.getProtocol())) {
            LocateRegistry.createRegistry(listen.getPort(), csf, ssf);
        }

        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(listen,
                                                                                env,
                                                                                mbs);
        cs.start();
        return cs;
    }

    public static void stop() {
        Optional.ofNullable(reporter).ifPresent(JmxReporter::stop);
        registered.forEach(t -> {
            try {
                mbs.unregisterMBean(t);
            } catch (MBeanRegistrationException
                            | InstanceNotFoundException ex) {
                // Ignored exception
            }
        });
        registered.clear();
        Optional.ofNullable(server).ifPresent(t -> {
            try {
                t.stop();
            } catch (IOException e) {
                // Ignored exception
            }
        });
        stopMetrics();
        mbs = null;
        server = null;
    }

    public static void stopMetrics() {
        Optional.ofNullable(reporter).ifPresent(JmxReporter::close);
        reporter = null;
    }

    private static RMIServerSocketFactory getServerSocketFactory(Configuration conf) {
        ServerSocketFactory ssf = conf.sslContext.getServerSocketFactory();
        SSLParameters ssp = conf.sslParams;
        return p -> {
            SSLServerSocket ss = (SSLServerSocket) ssf.createServerSocket(p);
            ss.setSSLParameters(ssp);
            ss.setSoTimeout(5000);
            return ss;
        };
    }

    private JmxService() {
        // Intentionally kept empty
    }
}
