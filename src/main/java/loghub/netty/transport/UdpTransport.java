package loghub.netty.transport;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import loghub.ConnectionContext;

public class UdpTransport
        extends NettyTransport<InetSocketAddress>
        implements IpServices {

    UdpTransport(POLLER poller) {
        super(poller, TRANSPORT.UDP);
    }

    @Override
    public InetSocketAddress resolveAddress(TransportConfig config) {
        return IpServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    public void configureAbstractBootStrap(AbstractBootstrap<?, ?> bootstrap, TransportConfig config) {
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
    }

    @Override
    public void addSslHandler(TransportConfig config, ChannelPipeline pipeline, Logger logger) {
        IpServices.super.addSslHandler(config, pipeline, logger);
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
        super.configureBootStrap(bootstrap, config);
    }

}
