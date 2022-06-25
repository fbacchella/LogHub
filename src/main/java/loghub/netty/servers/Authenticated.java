package loghub.netty.servers;

import loghub.security.AuthenticationHandler;

public interface Authenticated {
    AuthenticationHandler getAuthHandler();
}
