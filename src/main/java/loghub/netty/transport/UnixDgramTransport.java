package loghub.netty.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainDatagramPacket;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;
import loghub.DomainConnectionContext;

public class UnixDgramTransport
        extends NettyTransport<DomainSocketAddress, DomainDatagramPacket>
        implements UnixDomainServices {

    protected UnixDgramTransport(POLLER poller) {
        super(poller, TRANSPORT.UNIX_STREAM);
    }

    @Override
    public DomainSocketAddress resolveAddress(TransportConfig config) {
        return UnixDomainServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, DomainDatagramPacket message) {
        DomainSocketAddress remoteaddr = message.sender();
        DomainSocketAddress localaddr = message.recipient();
        return new DomainConnectionContext(localaddr, remoteaddr);
    }

}
