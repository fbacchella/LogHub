package loghub.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public interface AlpnResolver {
    String RESOLVERNAME = "ALPNResolver";
    class AplNProcessing extends ChannelInboundHandlerAdapter {
        private final AlpnResolver consumer;
        public AplNProcessing(AlpnResolver consumer) {
            this.consumer = consumer;
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            consumer.insertAlpnPipeline(ctx);
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(msg);
        }
    }
    void insertAlpnPipeline(ChannelHandlerContext ctx);
}
