package loghub.netty.transport;

import java.net.SocketAddress;

import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;
import loghub.netty.ChannelConsumer;

public class UnixStreamTransport
        extends NettyTransport<DomainSocketAddress>
        implements ResolveUnixAddress {
    protected UnixStreamTransport(POLLER poller) {
        super(poller, TRANSPORT.UNIX_STREAM);
    }


    @Override
    public DomainSocketAddress resolveAddress(TransportConfig config) {
        return ResolveUnixAddress.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return ResolveUnixAddress.super.getNewConnectionContext(ctx);
    }

    @Override
    public void addChildhandlers(ChannelConsumer source, Logger logger) {

    }

}
