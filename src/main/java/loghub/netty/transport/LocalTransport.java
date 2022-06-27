package loghub.netty.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import loghub.ConnectionContext;

public class LocalTransport extends NettyTransport<LocalAddress, ByteBuf>{

    private static class LocalChannelConnectionContext extends ConnectionContext<LocalAddress> {
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

    LocalTransport(POLLER poller) {
        super(poller, TRANSPORT.LOCAL);
    }
    @Override
    protected LocalAddress resolveAddress(TransportConfig config) {
        return new LocalAddress(config.endpoint);
    }

    @Override
    public ConnectionContext<LocalAddress> getNewConnectionContext(ChannelHandlerContext ctx, ByteBuf m) {
        return new LocalChannelConnectionContext((LocalChannel) ctx.channel());
    }

}
