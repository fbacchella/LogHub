package loghub.security;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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

import loghub.security.ssl.ClientAuthentication;

public class AuthenticationHandler {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {
        private ClientAuthentication sslclient = null;
        private String jaasName = null;
        private Configuration jaasConfig;
        private String login;
        private char[] password;
        private boolean withSsl = false;
        private boolean active = false;
        private SSLEngine engine;

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
                active = true;
                try {
                    this.sslclient = ClientAuthentication.valueOf(sslclient.toUpperCase());
                } catch (IllegalArgumentException e) {
                    logger.throwing(Level.DEBUG, e);
                    throw new IllegalArgumentException(String.format("'%s' is not a valide value", sslclient), e);
                }
            }
            return this;
        }
        public Builder setSslContext(SSLContext sslctx) {
            engine = sslctx.createSSLEngine();
            engine.setUseClientMode(false);
            return this;
        }
        public Builder setLogin(String login) {
            active = active || (login != null && ! login.isEmpty());
            this.login = login;
            return this;
        }
        public Builder setPassword(char[] password) {
            active = active || (password != null && password.length > 0);
            this.password = password;
            return this;
        }
        public Builder setJaasConfig(Configuration jaasConfig) {
            this.jaasConfig = jaasConfig;
            return this;
        }
        public Builder setJaasName(String jaasName) {
            if ((jaasName != null && ! jaasName.isEmpty())) {
                this.jaasName = jaasName;
                active = true;
                if (jaasConfig.getAppConfigurationEntry(jaasName) == null){
                    throw new IllegalArgumentException(String.format("JAAS name '%s' not found", jaasName));
                }
            }
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

    private AuthenticationHandler(Builder builder) {
        if (builder.withSsl) {
            this.sslclient = builder.sslclient;
            builder.sslclient.configureEngine(builder.engine);
        } else {
            this.sslclient = null;
        }

        this.login = builder.login;
        this.password = builder.password;

        this.jaasName = builder.jaasName;
        this.jaasConfig = builder.jaasConfig;
    }

    public Principal checkSslClient(SSLSession sess) throws GeneralSecurityException {
        logger.trace("testing ssl client");
        if (sslclient != ClientAuthentication.NOTNEEDED) {
            try {
                if (sslclient == ClientAuthentication.WANTED || sslclient == ClientAuthentication.REQUIRED) {
                    return sess.getPeerPrincipal();
                }
            } catch (SSLPeerUnverifiedException e1) {
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
        logger.trace("testing login {}", login);
        if (login.equals(login) && Arrays.equals(password, tryPassword)) {
            return new JMXPrincipal(login);
        } else {
            return checkJaas(tryLogin, tryPassword);
        }
    }

    private Principal checkJaas(String tryLogin, char[] tryPassword) {
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

}
