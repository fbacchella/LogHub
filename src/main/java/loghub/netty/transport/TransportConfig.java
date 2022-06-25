package loghub.netty.transport;

import javax.net.ssl.SSLContext;

import loghub.netty.ChannelConsumer;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class TransportConfig {
    @Getter @Setter
    public int rcvBuf;
    @Getter @Setter
    public int sndBuf;
    @Getter @Setter
    String endpoint;
    @Getter @Setter
    int port;
    @Getter @Setter
    int backlog;
    @Getter @Setter
    int bufferSize;
    @Getter @Setter
    int workerThreads;
    @Getter @Setter
    ChannelConsumer consumer;
    @Getter @Setter
    AuthenticationHandler authHandler;
    @Getter @Setter
    SSLContext sslctx = null;
    @Getter @Setter
    String sslKeyAlias = null;
    @Getter @Setter
    ClientAuthentication sslClientAuthentication = null;
}
