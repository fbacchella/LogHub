package loghub.netty.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;
import loghub.netty.ChannelConsumer;

public class UnixDgramTransport
        extends NettyTransport<DomainSocketAddress>
        implements ResolveUnixAddress {

    protected UnixDgramTransport(POLLER poller) {
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
    protected void addChildhandlers(ChannelConsumer source, TransportConfig config) {

    }

}
