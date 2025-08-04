package loghub.netty.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import loghub.BuildableConnectionContext;
import loghub.ConnectionContext;
import loghub.cloners.Immutable;

@TransportEnum(TRANSPORT.LOCAL)
public class LocalTransport extends NettyTransport<LocalAddress, ByteBuf, LocalTransport, LocalTransport.Builder> {

    public static class Builder extends NettyTransport.Builder<LocalAddress, ByteBuf, LocalTransport, LocalTransport.Builder> {
        @Override
        public LocalTransport build() {
            this.poller = POLLER.LOCAL;
            return new LocalTransport(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Immutable
    private static class LocalChannelConnectionContext extends BuildableConnectionContext<LocalAddress> {
        private final LocalAddress local;
        private final LocalAddress remote;
        private LocalChannelConnectionContext(LocalChannel channel) {
            this.local = channel.localAddress();
            this.remote = channel.remoteAddress();
        }
        @Override
        public LocalAddress getLocalAddress() {
            return local;
        }
        @Override
        public LocalAddress getRemoteAddress() {
            return remote;
        }
    }

    protected LocalTransport(Builder builder) {
        super(builder);
    }

    @Override
    protected LocalAddress resolveAddress() {
        return new LocalAddress(endpoint);
    }

    @Override
    public ConnectionContext<LocalAddress> getNewConnectionContext(ChannelHandlerContext ctx, ByteBuf m) {
        return new LocalChannelConnectionContext((LocalChannel) ctx.channel());
    }

}
