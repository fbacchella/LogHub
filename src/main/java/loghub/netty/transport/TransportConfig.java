package loghub.netty.transport;

import javax.net.ssl.SSLContext;

import loghub.netty.ChannelConsumer;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Data
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
    boolean withSsl = false;
    @Getter @Setter
    SSLContext sslContext = null;
    @Getter @Setter
    String sslKeyAlias = null;
    @Getter @Setter
    ClientAuthentication sslClientAuthentication = null;
    @Getter @Setter
    String threadPrefix;
}
