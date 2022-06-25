package loghub.netty.transport;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollChannelOption;
import loghub.ConnectionContext;
import loghub.netty.ChannelConsumer;

public class UdpTransport
        extends NettyTransport<InetSocketAddress>
        implements ResolveIpAddress {

    UdpTransport(POLLER poller) {
        super(poller, TRANSPORT.UDP);
    }

    @Override
    public InetSocketAddress resolveAddress(TransportConfig config) {
        return ResolveIpAddress.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    public void configureAbstractBootStrap(AbstractBootstrap<?, ?> bootstrap, TransportConfig config) {
        ResolveIpAddress.super.configureAbstractBootStrap(bootstrap, config);
    }

    @Override
    public void addSslHandler(TransportConfig config, ChannelPipeline pipeline, Logger logger) {
        ResolveIpAddress.super.addSslHandler(config, pipeline, logger);
    }

    @Override
    protected void addChildhandlers(ChannelConsumer source, TransportConfig config) {

    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
        ResolveIpAddress.super.configureAbstractBootStrap(bootstrap, config);
        super.configureBootStrap(bootstrap, config);
    }

}
