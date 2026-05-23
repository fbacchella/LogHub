package loghub.netty;

import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCounted;
import loghub.BuildableConnectionContext;
import loghub.netty.transport.NettyTransport;

import static loghub.netty.NettyReceiver.CONNECTIONCONTEXTATTRIBUTE;

@Sharable
public class ContextExtractorGeneric<I, A extends SocketAddress> extends MessageToMessageDecoder<I> {

    public static final String NAME = "SourceResolver";

    private final BiFunction<ChannelHandlerContext, I, BuildableConnectionContext<A>> resolve;

    public ContextExtractorGeneric(Class<I> messageClass, BiFunction<ChannelHandlerContext, I, BuildableConnectionContext<A>> resolve) {
        super(messageClass);
        this.resolve = resolve;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, I msg, List<Object> out) {
        BuildableConnectionContext<A> cctx = resolve.apply(ctx, msg);
        ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        Optional.ofNullable(ctx.channel().attr(NettyTransport.PRINCIPALATTRIBUTE)).map(Attribute::get).ifPresent(cctx::setPrincipal);
        //The message is not transformed in this step, so don't decrease reference count
        if (msg instanceof ReferenceCounted rf) {
            rf.retain();
        }
        out.add(msg);
    }

}
