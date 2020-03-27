package loghub.netty;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCounted;

@Sharable
public class ContextExtractor<SM> extends MessageToMessageDecoder<SM> {
    
    public static final String NAME = "SourceResolver";

    private final NettyReceiver<?, ?, ?, ?, ?, ?, ?, ?, ?, SM> r;

    public ContextExtractor(Class<SM> messageClass, NettyReceiver<?, ?, ?, ?, ?, ?, ?, ?, ?, SM> r) {
        super(messageClass);
        assert r != null;
        this.r = r;
    }

    public ContextExtractor(NettyReceiver<?, ?, ?, ?, ?, ?, ?, ?, ?, SM> r) {
        assert r != null;
        this.r = r;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
        System.out.println(msg.getClass());
        r.makeConnectionContext(ctx, msg);
        //The message is not transformed in this step, so don't decrease reference count
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).retain();
        }
        out.add(msg);
    }
}
