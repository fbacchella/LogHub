package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpExpectationFailedEvent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import loghub.BuilderClass;
import loghub.Event;
import loghub.decoders.DecodeException;
import loghub.decoders.JournaldExport;
import loghub.metrics.Stats;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.HttpChannelConsumer;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;

@Blocking
@SelfDecoder
@BuilderClass(Journald.Builder.class)
public class Journald extends AbstractHttpReceiver<Journald, Journald.Builder> {

    private static final AttributeKey<Boolean> VALIDJOURNALD = AttributeKey.newInstance(Journald.class.getCanonicalName() + ".VALIDJOURNALD");
    private static final AttributeKey<List<Event>> EVENTS = AttributeKey.newInstance(Journald.class.getCanonicalName() + ".EVENTS");

    private static final FullHttpResponse BAD_REQUEST =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, wrapResponse("Require POST /upload with type application/vnd.fdo.journal"));

    /**
     * This aggregator swallows valid journald events, that are sent as chunk by systemd-journal-upload
     * Other parts (the header) and non-valid requests are forwarded as-is, to be handled by the usual processing
     * @author Fabrice Bacchella
     *
     */
    class JournaldAgregator extends HttpObjectAggregator {

        // Old the current list of events
        private List<Event> events;
        // This variable hold the state of the current stream
        // Once broken don't try to recover
        private boolean valid = false;
        private CompositeByteBuf chunksBuffer;
        // A local decoder as this aggregator is statefull, it should not be shared, event within a thread
        private final JournaldExport decoder = JournaldExport.getBuilder().build();

        public JournaldAgregator() {
            super(32768);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
            try {
                if (isStartMessage(msg)) {
                    processStart(ctx, msg, out);
                } else if (isContentMessage(msg) && valid) {
                    processContent(ctx, (HttpContent) msg, out);
                }
            } catch (DecodeException ex) {
                Journald.this.manageDecodeException(ex);
                streamFailure(ctx);
            } catch (Exception e) {
                streamFailure(ctx);
                throw e;
            }
        }

        @Override
        protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
            if (! valid) {
                pipeline.fireUserEventTriggered(HttpExpectationFailedEvent.INSTANCE);
                return BAD_REQUEST.retainedDuplicate();
            } else {
                return super.newContinueResponse(start, maxContentLength, pipeline);
            }
        }

        private void processStart(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
            Journald.this.logger.debug("New journald query: {}", msg);
            HttpRequest headers = (HttpRequest) msg;
            String contentType = Optional.ofNullable(headers.headers().get("Content-Type")).orElse("");
            String uri = headers.uri().replace("//", "/");
            HttpMethod method = headers.method();
            if ( ("application/vnd.fdo.journal".equals(contentType))
                            &&  HttpMethod.POST.equals(method)
                            && "/upload".equals(uri)) {
                valid = true;
            }
            events = new ArrayList<>();
            chunksBuffer = Unpooled.compositeBuffer();
            super.decode(ctx, msg, out);
        }

        private void processContent(ChannelHandlerContext ctx, HttpContent chunk, List<Object> out) throws Exception {
            Journald.this.logger.debug("New journald chunk of events, length {}", () -> chunk.content().readableBytes());
            ByteBuf chunkContent = chunk.content();
            if (chunkContent.readableBytes() != 0) {
                Stats.newReceivedMessage(Journald.this, chunkContent.readableBytes());
                chunksBuffer.addComponent(true, chunkContent);
                chunkContent.retain();
                decoder.decode(getConnectionContext(ctx), chunksBuffer)
                       .map(Event.class::cast)
                       .forEach(events::add);
                chunksBuffer.discardReadComponents();
            }
            // end of POST, clean everything and forward data
            if (isLastContentMessage(chunk)) {
                assert chunksBuffer.numComponents() == 0 : chunksBuffer.numComponents();
                assert chunksBuffer.readerIndex() == chunksBuffer.writerIndex();
                ctx.channel().attr(VALIDJOURNALD).set(valid);
                ctx.channel().attr(EVENTS).set(events);
                // Reset because the aggregator might be reused
                valid = false;
                events = null;
                chunksBuffer.discardReadComponents();
                chunksBuffer = null;
                super.decode(ctx, LastHttpContent.EMPTY_LAST_CONTENT, out);
            }
        }

        private void streamFailure(ChannelHandlerContext ctx) {
            events.clear();
            valid = false;
            ctx.channel().attr(VALIDJOURNALD).set(valid);
            ctx.channel().attr(EVENTS).set(Collections.emptyList());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            decoder.channelInactive();
            super.channelInactive(ctx);
        }

    }

    @ContentType("text/plain; charset=utf-8")
    @RequestAccept(methods = {"GET", "PUT", "POST"})
    private class JournaldUploadHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request,
                ChannelHandlerContext ctx)
                throws HttpRequestFailure {
            if (Boolean.TRUE.equals(ctx.channel().attr(VALIDJOURNALD).get())) {
                ctx.channel().attr(EVENTS).get().forEach(Journald.this::send);
                writeResponse(ctx, request, HttpResponseStatus.ACCEPTED, wrapResponse("OK.\n"), 4);
            } else {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Not a valid journald request");
            }
        }

    }

    public static class Builder extends AbstractHttpReceiver.Builder<Journald, Journald.Builder> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        @Override
        public Journald build() {
            return new Journald(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    protected Journald(Builder builder) {
        super(builder);
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "JournaldReceiver";
    }

    @Override
    protected void configureConsumer(HttpChannelConsumer.Builder builder) {
        builder.setAggregatorSupplier(JournaldAgregator::new);
        // journald-upload uses 16kiB chunk buffers, the default HttpServerCodec uses 8kiB bytebuf
        builder.setServerCodecSupplier(() -> new HttpServerCodec(512, 1024, 32768, true, 32768, false, false));
    }

    @Override
    protected void modelSetup(ChannelPipeline pipeline) {
        pipeline.addLast(new JournaldUploadHandler());
    }

    @Override
    public String getReceiverName() {
        return "Journald/" + getListen() + "/" + getPort();
    }

    private static ByteBuf wrapResponse(String message) {
        return Unpooled.copiedBuffer(message, StandardCharsets.UTF_8);
    }

}
