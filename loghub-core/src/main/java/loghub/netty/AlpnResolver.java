package loghub.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public interface AlpnResolver {
    String RESOLVERNAME = "ALPNResolver";
    class AplNProcessing extends ChannelInboundHandlerAdapter {
        private final AlpnResolver consumer;
        private boolean alpnConfigured;
        public AplNProcessing(AlpnResolver consumer) {
            this.consumer = consumer;
            this.alpnConfigured = false;
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (! alpnConfigured) {
                alpnConfigured = true;
                consumer.insertAlpnPipeline(ctx);
                ctx.fireChannelRead(msg);
            }
        }
    }
    void insertAlpnPipeline(ChannelHandlerContext ctx);
}
