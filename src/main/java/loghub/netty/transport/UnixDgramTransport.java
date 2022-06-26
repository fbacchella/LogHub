package loghub.netty.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;

public class UnixDgramTransport
        extends NettyTransport<DomainSocketAddress>
        implements UnixDomainServices {

    protected UnixDgramTransport(POLLER poller) {
        super(poller, TRANSPORT.UNIX_STREAM);
    }

    @Override
    public DomainSocketAddress resolveAddress(TransportConfig config) {
        return UnixDomainServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
        return UnixDomainServices.super.getNewConnectionContext(ctx);
    }

}
