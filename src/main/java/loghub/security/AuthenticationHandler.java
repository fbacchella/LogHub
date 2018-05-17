package loghub.security;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.auth0.jwt.exceptions.JWTVerificationException;

import loghub.security.ssl.ClientAuthentication;

public class AuthenticationHandler {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {
        private boolean active = false;
        private ClientAuthentication sslclient = ClientAuthentication.NONE;
        private String jaasName = null;
        private Configuration jaasConfig = null;
        private String login;
        private char[] password;
        private boolean withSsl = false;
        private boolean withJwt = false;
        private JWTHandler jwtHandler;

        private Builder() {
        }

        public Builder useSsl() {
            return useSsl(true);
        }
        public Builder useSsl(boolean useSsl) {
            active = active || useSsl;
            withSsl = useSsl;
            return this;
        }
        public Builder setSslClientAuthentication(String sslclient) {
            if (sslclient != null && ! sslclient.isEmpty()) {
                try {
                    this.sslclient = ClientAuthentication.valueOf(sslclient.toUpperCase());
                } catch (IllegalArgumentException e) {
                    logger.throwing(Level.DEBUG, e);
                    throw new IllegalArgumentException(String.format("'%s' is not a valide value", sslclient), e);
                }
            }
            return this;
        }
        public Builder setLogin(String login) {
            active = active || ((password != null && password.length > 0) && (login != null && ! login.isEmpty()));
            this.login = login;
            return this;
        }
        public Builder setPassword(char[] password) {
            active = active || ((password != null && password.length > 0) && (login != null && ! login.isEmpty()));
            this.password = password;
            return this;
        }
        public Builder setJaasConfig(Configuration jaasConfig) {
            this.jaasConfig = jaasConfig;
            return this;
        }
        public Builder setJaasName(String jaasName) {
            active = active || (jaasName != null && ! jaasName.isEmpty());
            this.jaasName = jaasName;
            return this;
        }
        public Builder useJwt(boolean useJwt) {
            active = active || useJwt;
            withJwt = useJwt;
            return this;
        }
        public Builder setJwtHandler(JWTHandler handler) {
            this.jwtHandler = handler;
            return this;
        }

        public AuthenticationHandler build() {
            if (active) {
                return new AuthenticationHandler(this);
            } else {
                return null;
            }
        }
    }

    private static final Logger logger = LogManager.getLogger();

    // SSL authentication
    private final ClientAuthentication sslclient;

    // Hard code login password
    private final String login;
    private final char[] password;

    // Jaas password
    private final String jaasName ;
    private final Configuration jaasConfig;

    //JWT authentication
    private final JWTHandler jwtHandler;

    private AuthenticationHandler(Builder builder) {
        if (builder.withSsl) {
            this.sslclient = builder.sslclient;
        } else {
            this.sslclient = ClientAuthentication.NONE;
        }

        this.login = builder.login;
        this.password = builder.password;

        if(builder.jaasConfig != null && (builder.jaasName != null && ! builder.jaasName.isEmpty())) {
            this.jaasName = builder.jaasName;
            this.jaasConfig = builder.jaasConfig;
            if (jaasConfig.getAppConfigurationEntry(jaasName) == null){
                throw new IllegalArgumentException(String.format("JAAS name '%s' not found", jaasName));
            }
        } else if (builder.jaasConfig == null && (builder.jaasName != null && ! builder.jaasName.isEmpty())) {
            throw new IllegalArgumentException(String.format("Missing JAAS configuration"));
        } else {
            this.jaasName = null;
            this.jaasConfig = null;
        }
        if (builder.withJwt) {
            this.jwtHandler = builder.jwtHandler;
        } else {
            this.jwtHandler = null;
        }
    }

    public Principal checkSslClient(SSLSession sess) throws GeneralSecurityException {
        logger.debug("testing ssl client authentication");
        if (sslclient != ClientAuthentication.NOTNEEDED) {
            try {
                if (sslclient == ClientAuthentication.WANTED || sslclient == ClientAuthentication.REQUIRED) {
                    return sess.getPeerPrincipal();
                }
            } catch (SSLPeerUnverifiedException e) {
                if (sslclient == ClientAuthentication.REQUIRED) {
                    throw new FailedLoginException("Client authentication required but failed");
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    public Principal checkLoginPassword(String tryLogin, char[] tryPassword) {
        logger.debug("testing login {}", tryLogin);
        if ("".equals(tryLogin) && isWithJwt()) {
            return checkJwt(new String(tryPassword));
        } else if (tryLogin.equals(login) && Arrays.equals(password, tryPassword)) {
            return new JMXPrincipal(login);
        } else if (jaasName != null){
            return checkJaas(tryLogin, tryPassword);
        } else {
            return null;
        }
    }

    private Principal checkJaas(String tryLogin, char[] tryPassword) {
        logger.debug("testing login {} with JAAS {}", tryLogin, jaasName);
        CallbackHandler cbHandler = new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback cb: callbacks) {
                    if (cb instanceof NameCallback) {
                        NameCallback nc = (NameCallback)cb;
                        nc.setName(tryLogin);
                    } else if (cb instanceof PasswordCallback) {
                        PasswordCallback pc = (PasswordCallback)cb;
                        pc.setPassword(tryPassword);
                    } else {
                        throw new UnsupportedCallbackException(cb, "Unrecognized Callback");
                    }
                }
            }
        };

        LoginContext lc;
        try {
            lc = new LoginContext(jaasName, null,
                    cbHandler,
                    jaasConfig);
        } catch (LoginException e) {
            logger.error("Unusable jaas profile {}: {}", jaasName, e.getMessage());
            logger.catching(Level.DEBUG, e);
            return null;
        }
        try {
            lc.login();
            return lc.getSubject().getPrincipals().stream().findFirst().orElse(null);
        } catch (LoginException e) {
            logger.error("Failed loging: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return null;
        }
    }

    public Principal checkJwt(String token) {
        if (jwtHandler != null) {
            logger.debug("testing JWT token");
            try {
                return jwtHandler.verifyToken(token);
            } catch (JWTVerificationException e) {
                logger.warn("Failed token {}: {}", token, e.getMessage());
                logger.catching(Level.DEBUG, e);
                return null;
            }
        } else {
            return null;
        }
    }

    public JWTHandler getJwtHandler() {
        return jwtHandler;
    }

    public boolean isWithJwt() {
        return jwtHandler != null;
    }

    public ClientAuthentication getClientAuthentication() {
        return sslclient;
    }

    public boolean useJaas() {
        return ! jaasName.isEmpty();
    }

    public String getJaasName() {
        return jaasName;
    }

}
