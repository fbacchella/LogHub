package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import loghub.configuration.Properties;
import loghub.netty.servers.NettyIpServer;
import lombok.Getter;
import lombok.Setter;

public abstract class NettyIpReceiver<R extends NettyIpReceiver<R, S, B, CF, BS, BSC, SC, CC, SM>,
                                      S extends NettyIpServer<CF, BS, BSC, SC, S, B>,
                                      B extends NettyIpServer.Builder<S, B, BS, BSC>,
                                      CF extends ComponentFactory<BS, BSC, InetSocketAddress>,
                                      BS extends AbstractBootstrap<BS,BSC>,
                                      BSC extends Channel,
                                      SC extends Channel,
                                      CC extends Channel,
                                      SM> extends NettyReceiver<R, S, B, CF, BS, BSC, SC, CC, InetSocketAddress, SM> {

    public abstract static class Builder<B extends NettyIpReceiver<?, ?, ?, ?, ?, ?, ?, ?, ?>> extends NettyReceiver.Builder<B> {
        @Setter
        private int port;
        @Setter
        private String host = null;
    };

    @Getter
    private final int port;
    @Getter
    private final String host;

   protected NettyIpReceiver(Builder<? extends NettyIpReceiver<R, S, B, CF, BS, BSC, SC, CC, SM>> builder) {
        super(builder);
        this.port = builder.port;
        // Ensure host is null if given empty string, to be resolved as "bind *" by InetSocketAddress;
        this.host = builder.host != null && !builder.host.isEmpty() ? builder.host : null;
    }

    @Override
    public boolean configure(Properties properties, B builder) {
        if (isWithSSL()) {
            builder.setSSLClientAuthentication(getSSLClientAuthentication())
                   .setSSLContext(properties.ssl)
                   .setSSLKeyAlias(getSSLKeyAlias())
                   .useSSL(isWithSSL());
        }
        builder.setPort(port)
               .setHost(host);
        return super.configure(properties, builder);
    }

}
