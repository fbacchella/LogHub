package loghub.netty.transport;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.sctp.SctpMessage;
import loghub.ConnectionContext;

@TransportEnum(TRANSPORT.SCTP)
public class SctpTransport
        extends AbstractIpTransport<SctpMessage, SctpTransport, SctpTransport.Builder> {

    public static class Builder extends AbstractIpTransport.Builder<SctpMessage, SctpTransport, SctpTransport.Builder> {
        @Override
        public SctpTransport build() {
            return new SctpTransport(this);
        }
    }
    public static SctpTransport.Builder getBuilder() {
        return new SctpTransport.Builder();
    }

    protected SctpTransport(Builder builder) {
        super(builder);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx,
            SctpMessage message) {
        return null;
    }

}
