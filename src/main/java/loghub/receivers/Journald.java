package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import loghub.BuilderClass;
import loghub.Event;
import loghub.decoders.DecodeException;
import loghub.decoders.JournaldExport;
import loghub.metrics.Stats;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;

@Blocking
@SelfDecoder
@BuilderClass(Journald.Builder.class)
public class Journald extends AbstractHttpReceiver {

    private static final AttributeKey<Boolean> VALIDJOURNALD = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + Boolean.class.getName());
    private static final AttributeKey<List<Event>> EVENTS = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + List.class.getName());

    private static final ThreadLocal<ByteBuf> OkResponse = ThreadLocal.withInitial( () -> Unpooled.copiedBuffer("OK.\n", StandardCharsets.UTF_8));

    /**
     * This aggregator swallows valid journald events, that are sent as chunk by systemd-journal-upload
     * Other parts (the header) and non-valid requests are forwarded as-is, to be handled by the usual processing
     * @author Fabrice Bacchella
     *
     */
    class JournaldAgregator extends HttpObjectAggregator {

        // Old the current list of events
        // They will be send only at the end of the batch, so journald-upload don't try to republish events in case of broken connection
        private final List<Event> events = new ArrayList<>();
        // This variable hold the state of the current stream
        // Once broken don't try to recover
        private boolean valid = false;
        private CompositeByteBuf chunksBuffer;
        // A local decoder as this decoder is statefull, it should not be shared, event within a single thread
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

        private void processStart(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
            Journald.this.logger.debug("New journald post {}", msg);
            HttpRequest headers = (HttpRequest) msg;
            String contentType = Optional.ofNullable(headers.headers().get("Content-Type")).orElse("");
            String uri = headers.uri().replace("//", "/");
            HttpMethod method = headers.method();
            if ( ("application/vnd.fdo.journal".equals(contentType))
                            &&  HttpMethod.POST.equals(method)
                            && "/upload".equals(uri)) {
                valid = true;
            }
            chunksBuffer = ctx.alloc().compositeBuffer();
            super.decode(ctx, msg, out);
        }

        private void processContent(ChannelHandlerContext ctx, HttpContent chunk, List<Object> out) throws Exception {
            Journald.this.logger.trace("New journald chunk of events, length {}", () -> chunk.content().readableBytes());
            ByteBuf chunkContent = chunk.content();
            Stats.newReceivedMessage(Journald.this, chunkContent.readableBytes());
            chunksBuffer.addComponent(true, chunkContent);
            chunkContent.retain();
            decoder.decode(getConnectionContext(ctx), chunksBuffer)
                   .map(m -> (Event) m)
                   .forEach(events::add);
            chunksBuffer.discardReadBytes();

            // end of POST, clean everything and forward data
            if (isLastContentMessage(chunk)) {
                chunksBuffer.release();
                ctx.channel().attr(VALIDJOURNALD).set(valid);
                if (valid) {
                    ctx.channel().attr(EVENTS).set(new ArrayList<>(events));
                }
                // Reset because the aggregator might be reused
                valid = false;
                events.clear();
                super.decode(ctx, LastHttpContent.EMPTY_LAST_CONTENT, out);
            }
        }

        private void streamFailure(ChannelHandlerContext ctx) {
            valid = false;
            ctx.channel().attr(VALIDJOURNALD).set(valid);
            events.clear();
        }

    }

    public static class Builder extends AbstractHttpReceiver.Builder<Journald> {
        @Override
        public Journald build() {
            return new Journald(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    protected Journald(Builder builder) {
        super(builder);
    }

    @ContentType("text/plain; charset=utf-8")
    @RequestAccept(methods = {"GET", "PUT", "POST"})
    private class JournaldUploadHandler extends HttpRequestProcessing {

        @Override
        protected void processRequest(FullHttpRequest request,
                                      ChannelHandlerContext ctx)
                                                      throws HttpRequestFailure {
            if (Boolean.FALSE.equals(ctx.channel().attr(VALIDJOURNALD).get())) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Not a valid journald request");
            } else {
                ctx.channel().attr(EVENTS).get().forEach(Journald.this::send);
                ByteBuf okbuf = OkResponse.get().readerIndex(0).retain();
                writeResponse(ctx, request, HttpResponseStatus.ACCEPTED, okbuf, 4);
            }
        }

    }

    @Override
    protected void settings(HttpReceiverServer.Builder builder) {
        super.settings(builder);
        builder.setAggregatorSupplier(() -> new JournaldAgregator()).setReceiveHandler(new JournaldUploadHandler()).setThreadPrefix("Journald");
    }

    @Override
    public String getReceiverName() {
        return "Journald/0.0.0.0/" + getPort();
    }

    public JournaldAgregator getAggregator() {
        return new JournaldAgregator();
    }

}
