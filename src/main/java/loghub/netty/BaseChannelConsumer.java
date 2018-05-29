package loghub.netty;

import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCounted;
import loghub.Event;
import loghub.Helpers;

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
            Event e = r.nettyMessageDecode(ctx, msg);
            if (e == null && closeOnError) {
                ctx.close();
            } else if (e != null){
                out.add(e);
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

    @Sharable
    private class ExceptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
                                    Throwable cause) throws Error {
            if (Helpers.isFatal(cause)) {
                throw (Error) cause;
            }
            logger.error("Unmannageded exception: {}", cause.getMessage());
            logger.debug("details", cause);
            if (closeOnError) {
                ctx.close();
            }
        }
    }

    private final MessageToMessageDecoder<SM> nettydecoder;
    private final MessageToMessageDecoder<SM> extractor = new ContextExtractor();
    private final ChannelInboundHandlerAdapter exceptionhandler = new ExceptionHandler();
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
        p.addLast("ExceptionHandler", exceptionhandler);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Pipeline processing failed: {}", cause.getCause());
        logger.catching(Level.DEBUG, cause);
    }

}
