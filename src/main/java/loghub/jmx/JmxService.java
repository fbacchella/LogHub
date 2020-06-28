package loghub.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.security.Principal;
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
import javax.net.ssl.SSLContext;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import javax.security.auth.Subject;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;

import loghub.Helpers;
import loghub.security.AuthenticationHandler;
import lombok.Setter;
import lombok.experimental.Accessors;

public class JmxService {

    static final public PROTOCOL DEFAULTPROTOCOL = PROTOCOL.rmi;
    public static enum PROTOCOL {
        rmi,
        jmxmp,
    }

    public static Configuration configuration() {
        return new Configuration();
    }

    private final static Set<ObjectName> registred = new HashSet<>();

    @Accessors(chain = true)
    public static class Configuration {
        @Setter
        private PROTOCOL protocol = DEFAULTPROTOCOL;
        @Setter
        private int port = -1;
        @Setter
        private String hostname = null;
        // Mainly used for tests
        @Setter
        private SSLContext sslContext = null;
        @Setter
        private boolean withSsl = false;
        @Setter
        private String jaasName = null;
        @Setter
        private javax.security.auth.login.Configuration jaasConfig = null;
        @Setter
        private MetricRegistry metrics = null;

        private final Map<ObjectName, Object> mbeans = new HashMap<>();

        private Configuration() { }

        public Configuration setProperties(Map<String, Object> values) {
            values.forEach(this::setProperty);
            return this;
        }

        public Configuration setProperty(String key, Object value) {
            switch(key) {
            case "protocol": 
                protocol = PROTOCOL.valueOf(value.toString().toLowerCase(Locale.ENGLISH));
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
            default:
                throw new IllegalArgumentException("Unknown property " + key);
            }
            return this;
        }

        public Configuration register(ObjectName key, Object value) {
            mbeans.put(key, value);
            return this;
        }

    }

    private static JmxReporter reporter;
    private static MBeanServer mbs;
    private static JMXConnectorServer server;

    public static void start(Configuration conf) throws IOException {
        mbs = ManagementFactory.getPlatformMBeanServer();

        startJmxReporter(conf);

        try {
            conf.mbeans.forEach((k,v) -> {
                try {
                    mbs.registerMBean(v,k);
                    registred.add(k);
                } catch (InstanceAlreadyExistsException
                                | MBeanRegistrationException
                                | NotCompliantMBeanException ex) {
                    throw new UndeclaredThrowableException(ex);
                }
            });
        } catch (UndeclaredThrowableException ex) {
            throw new IllegalStateException("Unusuable JMX setup: " + Helpers.resolveThrowableException(ex.getCause()), ex);
        }

        if (conf.port >= 0) {
            server = startConnectorServer(conf);
        }
    }

    private static void startJmxReporter(Configuration conf) {
        ObjectNameFactory donf = new DefaultObjectNameFactory();
        Pattern pipepattern = Pattern.compile("^([^\\.]+)\\.(.+?)\\.([a-zA-z0-9]+)$");
        reporter = JmxReporter.forRegistry(conf.metrics).createsObjectNamesWith(new ObjectNameFactory() {
            @Override
            public ObjectName createName(String type, String domain, String name) {
                Matcher m = pipepattern.matcher(name);
                if (m.matches()) {
                    String service = m.group(1);
                    String servicename = m.group(2);
                    String metric = m.group(3);
                    Hashtable<String, String> table = new Hashtable<>(3);
                    table.put("type", service);
                    table.put("servicename", ObjectName.quote(servicename));
                    table.put("name", metric);
                    try {
                        return new ObjectName("loghub", table);
                    } catch (MalformedObjectNameException e) {
                        return donf.createName(type, domain, name);
                    }
                } else {
                    return donf.createName(type, domain, name);
                }
            }
        }).registerWith(mbs).build();
        reporter.start();
    }

    private static JMXConnectorServer startConnectorServer(Configuration conf) throws IOException {
        Map<String, Object> env = new HashMap<>();
        RMIClientSocketFactory csf = null;
        RMIServerSocketFactory ssf = null;
        if (conf.hostname != null) {
            System.setProperty("java.rmi.server.hostname", conf.hostname);
            System.setProperty("java.rmi.server.useLocalHostname", "false");
        }
        if (conf.withSsl) {
            ssf = new SslRMIServerSocketFactory(conf.sslContext, null, null, false);
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);
        }
        if (conf.jaasName != null) {
            AuthenticationHandler ah = AuthenticationHandler.getBuilder()
                            .setJaasName(conf.jaasName)
                            .setJaasConfig(conf.jaasConfig)
                            .build();

            env.put(JMXConnectorServer.AUTHENTICATOR,
                    new JMXAuthenticator() {
                @Override
                public Subject authenticate(Object credentials) {

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
                }
            });
        }
        String path = "/";
        if (conf.protocol == PROTOCOL.rmi) {
            java.rmi.registry.LocateRegistry.createRegistry(conf.port, csf, ssf);
            path = "/jndi/rmi://0.0.0.0:" + conf.port + "/jmxrmi";
        }
        JMXServiceURL url = new JMXServiceURL(conf.protocol.toString(), "0.0.0.0",
                                              conf.port, path);
        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(url,
                                                                                env,
                                                                                mbs);
        cs.start();
        return cs;
    }

    public static void stop() {
        Optional.ofNullable(reporter).ifPresent(JmxReporter::stop);
        registred.forEach(t -> {
            try {
                mbs.unregisterMBean(t);
            } catch (MBeanRegistrationException
                            | InstanceNotFoundException e1) {
            }
        });
        registred.clear();
        Optional.ofNullable(server).ifPresent(t -> {
            try {
                t.stop();
            } catch (IOException e) {
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

}
