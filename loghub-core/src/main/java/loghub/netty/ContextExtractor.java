package loghub.netty;

import java.util.List;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCounted;

@Sharable
public class ContextExtractor<R extends NettyReceiver<R, SM, B>, SM, B extends NettyReceiver.Builder<R, SM, B>> extends MessageToMessageDecoder<SM> {

    public static final String NAME = "SourceResolver";

    private final NettyReceiver<R, SM, B> r;

    public ContextExtractor(Class<SM> messageClass, NettyReceiver<R, SM, B> r) {
        super(messageClass);
        this.r = r;
    }

    public ContextExtractor(NettyReceiver<R, SM, B> r) {
        this.r = r;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
        // In datagram-oriented flow, the remote adress is in the packet, not the channel
        // So forward the message too for check.
        r.makeConnectionContext(ctx, msg);
        //The message is not transformed in this step, so don't decrease reference count
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).retain();
        }
        out.add(msg);
    }

}
