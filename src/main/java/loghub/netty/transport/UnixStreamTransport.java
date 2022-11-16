package loghub.netty.transport;

import java.net.SocketAddress;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;

@TransportEnum(TRANSPORT.UNIX_STREAM)
public class UnixStreamTransport extends
        AbstractUnixDomainTransport<ByteBuf, UnixStreamTransport, UnixStreamTransport.Builder> {

    public static class Builder extends AbstractUnixDomainTransport.Builder<ByteBuf, UnixStreamTransport, UnixStreamTransport.Builder> {
        @Override
        public UnixStreamTransport build() {
            return new UnixStreamTransport(this);
        }
    }
    public static UnixStreamTransport.Builder getBuilder() {
        return new UnixStreamTransport.Builder();
    }

    private UnixStreamTransport(UnixStreamTransport.Builder builder) {
        super(builder);
    }
    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, ByteBuf message) {
        SocketAddress remoteChannelAddr = ctx.channel().remoteAddress();
        SocketAddress localChannelAddr = ctx.channel().localAddress();
        DomainSocketAddress remoteaddr = null;
        DomainSocketAddress localaddr = null;
        if (remoteChannelAddr instanceof DomainSocketAddress) {
            remoteaddr = (DomainSocketAddress)remoteChannelAddr;
        }
        if (localChannelAddr instanceof DomainSocketAddress) {
            localaddr = (DomainSocketAddress)localChannelAddr;
        }

        return new DomainConnectionContext(localaddr, remoteaddr);
    }

}
