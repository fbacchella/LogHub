package loghub.netty.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainDatagramPacket;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;

@TransportEnum(TRANSPORT.UNIX_DGRAM)
public class UnixDgramTransport extends
        AbstractUnixDomainTransport<DomainDatagramPacket, UnixDgramTransport, UnixDgramTransport.Builder> {

    public static class Builder extends AbstractUnixDomainTransport.Builder<DomainDatagramPacket, UnixDgramTransport, UnixDgramTransport.Builder> {
        @Override
        public UnixDgramTransport build() {
            return new UnixDgramTransport(this);
        }
    }
    public static UnixDgramTransport.Builder getBuilder() {
        return new UnixDgramTransport.Builder();
    }

    private UnixDgramTransport(UnixDgramTransport.Builder builder) {
        super(builder);
    }

    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, DomainDatagramPacket message) {
        DomainSocketAddress remoteaddr = message.sender();
        DomainSocketAddress localaddr = message.recipient();
        return new DomainConnectionContext(localaddr, remoteaddr);
    }

}
