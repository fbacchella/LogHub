package loghub.jmx;

import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
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

import loghub.security.AuthenticationHandler;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class JmxServerBuilder {

    static final public PROTOCOL DEFAULTPROTOCOL = PROTOCOL.rmi;
    public static enum PROTOCOL {
        rmi,
        jmxmp,
    }

    public static JmxServerBuilder builder() {
        return new JmxServerBuilder();
    }

    @Setter
    private PROTOCOL protocol = DEFAULTPROTOCOL;
    @Setter
    private int port;
    @Setter
    private String hostname = null;
    // Mainly used for tests
    @Setter
    private MBeanServer mbs = null;
    @Setter
    private SSLContext sslContext = null;
    @Setter
    private boolean withSsl = false;
    @Setter
    private String jaasName = null;
    @Setter
    private javax.security.auth.login.Configuration jaasConfig = null;

    private final Map<ObjectName, Object> mbeans = new HashMap<>();

    private JmxServerBuilder() { }

    public JmxServerBuilder setProperties(Map<String, Object> values) {
        values.forEach(this::setProperty);
        return this;
    }

    public JmxServerBuilder setProperty(String key, Object value) {
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
        }
        return this;
    }

    public JmxServerBuilder register(ObjectName key, Object value) {
        mbeans.put(key, value);
        return this;
    }

    public JMXConnectorServer start() throws Exception {
        if (port < 0) {
            return null;
        }

        Map<String, Object> env = new HashMap<>();

        RMIClientSocketFactory csf = null;
        RMIServerSocketFactory ssf = null;

        if (hostname != null) {
            System.setProperty("java.rmi.server.hostname", hostname);
            System.setProperty("java.rmi.server.useLocalHostname", "false");
        }

        if (withSsl) {
            ssf = new SslRMIServerSocketFactory(sslContext, null, null, false);
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);
        }

        if (jaasName != null) {
            AuthenticationHandler ah = AuthenticationHandler.getBuilder()
                            .setJaasName(jaasName)
                            .setJaasConfig(jaasConfig)
                            .build();

            env.put(JMXConnectorServer.AUTHENTICATOR, new JMXAuthenticator() {
                @Override
                public Subject authenticate(Object credentials) {

                    Principal p = null;
                    if ((credentials instanceof String[])) {
                        String[] loginPassword = (String[]) credentials;
                        if (loginPassword.length == 2) {
                            p = ah.checkLoginPassword(loginPassword[0], loginPassword[1].toCharArray());
                            loginPassword[1] = null;
                            if (p == null) {
                                throw new SecurityException("Invalid user");
                            }
                        }
                    }
                    if (p == null) {
                        throw new SecurityException("Invalid configuration");
                    } else {
                        return new Subject(true, Collections.singleton(p), Collections.emptySet(), Collections.emptySet());
                    }
                }
            });
        }

        String path = "/";
        if (protocol == PROTOCOL.rmi) {
            java.rmi.registry.LocateRegistry.createRegistry(port, csf, ssf);
            path = "/jndi/rmi://0.0.0.0:" + port + "/jmxrmi";
        }
        JMXServiceURL url = new JMXServiceURL(protocol.toString(), "0.0.0.0", port, path);
        if (mbs == null) {
            mbs = ManagementFactory.getPlatformMBeanServer();
        }
        try {
            mbeans.forEach((k,v) -> {
                try {
                    mbs.registerMBean(v,k);
                } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException ex) {
                    throw new UndeclaredThrowableException(ex);
                }
            });
        } catch (UndeclaredThrowableException ex) {
            throw (Exception) ex.getCause();
        }
        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
        cs.start();
        return cs;
    }

}
