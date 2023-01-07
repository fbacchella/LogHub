package loghub.zmq;

import java.security.Principal;

import javax.security.auth.login.Configuration;

import loghub.security.AuthenticationHandler;
import zmq.io.mechanism.Mechanisms;

public interface ZapDomainHandler {

    enum ZapDomainHandlerProvider {
        ALLOW {
            @Override
            ZapDomainHandler get(ZMQSocketFactory factory, Mechanisms security) {
                return ZapDomainHandler.ALLOW;
            }
        },
        DENY {
            @Override
            ZapDomainHandler get(ZMQSocketFactory factory, Mechanisms security) {
                return ZapDomainHandler.DENY;
            }
        },
        STRICT {
            @Override
            ZapDomainHandler get(ZMQSocketFactory zfactory, Mechanisms security) {
                if (security == Mechanisms.CURVE) {
                    return zfactory.getCertDirectoryFilter();
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        },
        METADATA {
            @Override
            ZapDomainHandler get(ZMQSocketFactory zfactory, Mechanisms security) {
                if (security == Mechanisms.CURVE) {
                    ZapDomainHandler certDirFilter = zfactory.getCertDirectoryFilter();
                    return r -> {
                        // Process the request, so certificate metadata can be added
                        certDirFilter.authorize(r);
                        return true;
                    };
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        ;
        abstract ZapDomainHandler get(ZMQSocketFactory factory, Mechanisms security);
    }

    ZapDomainHandler ALLOW = r -> true;
    ZapDomainHandler DENY = r -> false;

    boolean authorize(ZapRequest request);

    class JaasHandler implements ZapDomainHandler {
        private final AuthenticationHandler authenticationHandler;

        JaasHandler(Configuration jaasConfig, String domain) {
             authenticationHandler = AuthenticationHandler.getBuilder()
                                                          .setJaasConfig(jaasConfig)
                                                          .setJaasName(domain)
                                                          .build();

        }

        @Override
        public boolean authorize(ZapRequest request) {
            if (request.getMechanism() != Mechanisms.PLAIN) {
                throw new IllegalArgumentException();
            }
            Principal principal = authenticationHandler.checkLoginPassword(request.getUsername(), request.getPassword().toCharArray());
            if (principal != null) {
                request.setIdentity(principal.getName());
            }
            return principal != null;
        }
    }

}
