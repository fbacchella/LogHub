package loghub.netty;

import java.util.List;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCounted;
import loghub.Event;
import loghub.Helpers;
import loghub.receivers.SelfDecoder;

public class BaseChannelConsumer<R extends NettyReceiver<?, ?, ?, ?, BS, BSC, ?, ?, ?, SM>,
                                     BS extends AbstractBootstrap<BS,BSC>, 
                                     BSC extends Channel,
                                     SM> implements ChannelConsumer<BS, BSC> {

    private static final Logger logger = LogManager.getLogger();

    @Sharable
    private class EventSender extends SimpleChannelInboundHandler<Event> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Event ev) throws Exception {
            r.nettySend(ev);
        }
    }

    @Sharable
    private class LogHubDecoder extends MessageToMessageDecoder<SM> {
        @Override
        protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
            Stream<Event> es = r.nettyMessageDecode(ctx, msg);
            if (es == null && closeOnError) {
                ctx.close();
            } else if (es != null){
                es.forEach(out::add);
            }
        }
    }

    @Sharable
    private class ContextExtractor extends MessageToMessageDecoder<SM> {
        @Override
        protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
            // Calls getConnectionContext to ensure that the attribute is present
            r.getConnectionContext(ctx, msg);
            //The message is not transformed in this step, so don't decrease reference count
            if (msg instanceof ReferenceCounted) {
                ((ReferenceCounted) msg).retain();
            }
            out.add(msg);
        }
    }

    private final MessageToMessageDecoder<SM> nettydecoder;
    private final MessageToMessageDecoder<SM> extractor = new ContextExtractor();
    private final EventSender sender = new EventSender();
    private final boolean closeOnError;
    private final boolean selfDecoder;
    protected final R r;

    public BaseChannelConsumer(R r) {
        closeOnError = r.getClass().isAnnotationPresent(CloseOnError.class);
        selfDecoder = r.getClass().isAnnotationPresent(SelfDecoder.class);
        this.r = r;
        // Prepare the Netty decoder, before it's used during server creation in #getServer()
        if (! selfDecoder) {
            nettydecoder = new LogHubDecoder();
        } else {
            nettydecoder = null;
        }
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addFirst("SourceResolver", extractor);
        if (! selfDecoder) {
            p.addLast("MessageDecoder", nettydecoder);
        }
        p.addLast("Sender", sender);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Pipeline processing failed: {}", Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
    }

}
