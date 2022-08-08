package loghub.netty.transport;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;

@TransportEnum(TRANSPORT.UDP)
public class UdpTransport extends AbstractIpTransport<DatagramPacket, UdpTransport, UdpTransport.Builder> {

    public static class Builder extends AbstractIpTransport.Builder<DatagramPacket, UdpTransport, UdpTransport.Builder> {
        @Override
        public UdpTransport build() {
            return new UdpTransport(this);
        }
    }
    public static UdpTransport.Builder getBuilder() {
        return new UdpTransport.Builder();
    }

    private UdpTransport(UdpTransport.Builder builder) {
        super(builder);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, DatagramPacket message) {
        InetSocketAddress remoteaddr = message.sender();
        InetSocketAddress localaddr = message.recipient();
        return new IpConnectionContext(localaddr, remoteaddr, null);
    }

}
