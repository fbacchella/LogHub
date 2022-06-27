package loghub.netty.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;

public class UnixStreamTransport
        extends NettyTransport<DomainSocketAddress, ByteBuf>
        implements UnixDomainServices {
    protected UnixStreamTransport(POLLER poller) {
        super(poller, TRANSPORT.UNIX_STREAM);
    }


    @Override
    public DomainSocketAddress resolveAddress(TransportConfig config) {
        return UnixDomainServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, ByteBuf message) {
        return UnixDomainServices.super.getNewConnectionContext(ctx);
    }

}
