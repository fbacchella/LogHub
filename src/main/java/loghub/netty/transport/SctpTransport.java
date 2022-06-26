package loghub.netty.transport;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import loghub.ConnectionContext;

public class SctpTransport
        extends NettyTransport<InetSocketAddress>
        implements IpServices {

    SctpTransport(POLLER poller) {
        super(poller, TRANSPORT.SCTP);
    }

    @Override
    public InetSocketAddress resolveAddress(TransportConfig config) {
        return IpServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return IpServices.super.getNewConnectionContext(ctx);
    }

    @Override
    protected void configureServerBootStrap(ServerBootstrap bootstrap, TransportConfig config) {
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
        super.configureServerBootStrap(bootstrap, config);
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
        super.configureBootStrap(bootstrap, config);
    }

}
