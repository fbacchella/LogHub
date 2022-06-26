package loghub.netty.transport;

import java.net.SocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.DomainSocketAddress;
import loghub.ConnectionContext;
import loghub.DomainConnectionContext;

public interface UnixDomainServices {


    default DomainSocketAddress resolveAddress(TransportConfig config) {
        return new DomainSocketAddress(config.endpoint);
    }

    default ConnectionContext<DomainSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx) {
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
