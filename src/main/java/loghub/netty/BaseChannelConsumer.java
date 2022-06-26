package loghub.netty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import loghub.Event;
import loghub.FilterException;
import loghub.Helpers;
import loghub.decoders.DecodeException;
import loghub.receivers.SelfDecoder;

public class BaseChannelConsumer<R extends NettyReceiver<SM>, SM> implements ChannelConsumer {

    private static final Logger logger = LogManager.getLogger();

    @Sharable
    private class EventSender extends SimpleChannelInboundHandler<Event> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Event ev) throws Exception {
            r.nettySend(ev);
        }
    }

    @Sharable
    private class LogHubDecoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
            Stream<Event> es = r.nettyMessageDecode(ctx, msg);
            if (es == null && closeOnError) {
                ctx.close();
            } else if (es != null){
                es.forEach(out::add);
            }
        }
    }

    @Sharable
    private class FilterHandler extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
            try {
                out.add(r.getFilter().filter(msg));
            } catch (FilterException ex) {
                BaseChannelConsumer.this.r.manageDecodeException(new DecodeException(ex));
            }
        }
    }

    @Sharable
    private class ContentExtractor extends MessageToMessageDecoder<SM> {
        @Override
        protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
            ByteBuf content = r.getContent(msg);
            // Often the content and the message are linked.
            // To avoid a useless copy, keep the message.
            if (content.equals(msg)) {
                content.retain();
            } else if (msg instanceof ByteBufHolder) {
                ((ByteBufHolder)msg).retain();
            }
            out.add(content);
        }
    }

    private class LocalContextExtractor extends ContextExtractor<SM> {
        public LocalContextExtractor(NettyReceiver<SM> r) {
            super(r);
        }
    }

    private final ContextExtractor<SM> extractor;
    private final Optional<MessageToMessageDecoder<ByteBuf>> filter;
    private final Optional<MessageToMessageDecoder<ByteBuf>> nettydecoder;
    private final EventSender sender = new EventSender();
    private final boolean closeOnError;
    protected final R r;

    public BaseChannelConsumer(R r) {
        closeOnError = r.getClass().isAnnotationPresent(CloseOnError.class);
        this.r = r;
        extractor = new LocalContextExtractor(r);
        // Some filters are sharable, so keep them
        filter = Optional.ofNullable(r.getFilter()).map(i -> new FilterHandler());
        nettydecoder = Optional.of(r.getClass()).filter(i -> ! i.isAnnotationPresent(SelfDecoder.class)).map(i -> new LogHubDecoder());
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addFirst(ContextExtractor.NAME, extractor);
        p.addLast("ContentExtractor", new ContentExtractor());
        filter.ifPresent(i -> p.addLast("Filter", i));
        nettydecoder.ifPresent(i -> p.addLast("MessageDecoder", i));
        p.addLast("Sender", sender);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Pipeline processing failed: {}", () -> Helpers.resolveThrowableException(cause));
        logger.catching(Level.DEBUG, cause);
    }

    @Override
    public void logFatalException(Throwable ex) {
        logger.fatal("Caught fatal exception", ex);
    }

}
