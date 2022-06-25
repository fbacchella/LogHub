package loghub.netty.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import loghub.ConnectionContext;
import loghub.netty.ChannelConsumer;

public class SctpTransport
        extends NettyTransport<InetSocketAddress>
        implements ResolveIpAddress {

    SctpTransport(POLLER poller) {
        super(poller, TRANSPORT.SCTP);
    }

    @Override
    public void addChildhandlers(ChannelConsumer source, Logger logger) {

    }

    @Override
    public InetSocketAddress resolveAddress(TransportConfig config) {
        return ResolveIpAddress.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return ResolveIpAddress.super.getNewConnectionContext(ctx);
    }

    @Override
    protected void configureServerBootStrap(ServerBootstrap bootstrap, TransportConfig config) {
        ResolveIpAddress.super.configureAbstractBootStrap(bootstrap, config);
        super.configureServerBootStrap(bootstrap, config);
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
        ResolveIpAddress.super.configureAbstractBootStrap(bootstrap, config);
        super.configureBootStrap(bootstrap, config);
    }

}
