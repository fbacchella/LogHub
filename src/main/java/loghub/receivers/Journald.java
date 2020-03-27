package loghub.receivers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import io.netty.util.ByteProcessor;
import io.netty.util.ByteProcessor.IndexOfProcessor;
import loghub.BuilderClass;
import loghub.Event;
import loghub.decoders.DecodeException;
import loghub.netty.AbstractHttp;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;

@Blocking
@SelfDecoder
@BuilderClass(Journald.Builder.class)
public class Journald extends AbstractHttp {

    private static final AttributeKey<Boolean> VALIDJOURNALD = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + Boolean.class.getName());
    private static final AttributeKey<List<Event>> EVENTS = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + List.class.getName());

    private static class BufferHolder {
        private CharBuffer cbuf = CharBuffer.allocate(256);
        private ByteBuffer bbuf = ByteBuffer.allocate(256);
        private CharBuffer getCharBuffer(int size) {
            if (size > cbuf.capacity()) {
                cbuf = CharBuffer.allocate(size);
            }
            cbuf.clear();
            cbuf.limit(size);
            return cbuf;
        }
        private ByteBuffer getByteBuffer(int size) {
            if (size > bbuf.capacity()) {
                bbuf = ByteBuffer.allocate(size);
            }
            bbuf.clear();
            bbuf.limit(size);
            return bbuf;
        }
    }

    private static final ThreadLocal<CharsetDecoder> utf8decoder = ThreadLocal.withInitial( () -> {
        return StandardCharsets.UTF_8.newDecoder().onUnmappableCharacter(CodingErrorAction.REPORT).onMalformedInput(CodingErrorAction.REPORT);
    });
    private static final ThreadLocal<BufferHolder> bufferolder = ThreadLocal.withInitial(BufferHolder::new);
    private static final ThreadLocal<ByteBuf> OkResponse = ThreadLocal.withInitial( () -> Unpooled.copiedBuffer("OK.\n", StandardCharsets.UTF_8));
    private static final String TRUSTEDFIELDS = "fields_trusted";
    private static final String USERDFIELDS = "fields_user";
    private static final ByteProcessor FIND_EQUAL = new IndexOfProcessor((byte)'=');
    private static final ByteProcessor NON_UNDERSCORE = new ByteProcessor() {
        @Override
        public boolean process(byte value) throws Exception {
            return value == (byte) '_';
        }
    };
    private static final Pattern ANSIPATTERN = Pattern.compile("\u001B\\[[;\\d]*[ -/]*[@-~]");

    /**
     * This aggregator swallows valid journald events, that are sended as chunk by systemd-journal-upload
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
        private final Map<String, HashMap<String, Object>> eventVars;
        private CompositeByteBuf chunksBuffer;

        public JournaldAgregator() {
            super(32768);
            eventVars = new HashMap<>(2);
            eventVars.put(USERDFIELDS, new HashMap<String, Object>());
            eventVars.put(TRUSTEDFIELDS, new HashMap<String, Object>());
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
            chunksBuffer.addComponent(true, chunkContent);
            chunkContent.retain();
            // Parse content as a journal export format event
            // See https://www.freedesktop.org/wiki/Software/systemd/export/ for specifications
            int eolPos;
            while ((eolPos = findEndOfLine(chunksBuffer)) >= 0) {
                int lineStart = chunksBuffer.readerIndex();
                ByteBuf lineBuffer;
                lineBuffer = chunksBuffer.readSlice(eolPos);
                // Read the EOL
                chunksBuffer.readByte();
                if (eolPos == 0) {
                    // An empty line, event separator
                    newEvent(ctx, eventVars);
                } else {
                    // Fields are extracted in place, to avoid many useless strings copy

                    // Resolve the key name
                    int equalPos = lineBuffer.forEachByte(FIND_EQUAL);
                    ByteBuf keyBuffer = equalPos > 0 ? lineBuffer.readSlice(equalPos) : lineBuffer.slice();
                    // Used to detect the number of _ in front of a field name
                    // 1, it's a trusted field, managed by journald
                    // 2, it's a private field, probably to be dropped
                    // Fields are explained at https://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
                    int startKey = keyBuffer.forEachByte(NON_UNDERSCORE);
                    keyBuffer.readerIndex(startKey);
                    String key = keyBuffer.toString(StandardCharsets.UTF_8).toLowerCase(Locale.ENGLISH);

                    boolean userField = startKey == 0;

                    if (equalPos > 0) {
                        // '=' found, simple key value case

                        if (startKey == 2) {
                            // fields starting with __ are privates, skip them
                            continue;
                        }
                        // A equal was found, a simple textual field
                        lineBuffer.readerIndex(equalPos + 1); // Skip the '='
                        String value = lineBuffer.toString(StandardCharsets.UTF_8);
                        eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, value);
                    } else {
                        // A binary field
                        int size = -1;
                        if (chunksBuffer.readableBytes() > 8 ) {
                            long contentSize = chunksBuffer.readLongLE();
                            try {
                                size = Math.toIntExact(contentSize);
                            } catch (ArithmeticException ex) {
                                throw new DecodeException("Binary field size overflow: " + contentSize, ex);
                            }
                        }
                        if (size > 0 && chunksBuffer.readableBytes() > size) {
                            if (startKey == 2) {
                                chunksBuffer.skipBytes(size);
                                // Read the EOL
                                chunksBuffer.readByte();
                                // fields starting with __ are privates, skip them, but after reading the binary part
                                continue;
                            } else {
                                String value = readBinary(size, chunksBuffer);
                                // Read the EOL
                                chunksBuffer.readByte();
                                eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, value);
                            }
                        } else {
                            //If overlap a chunk limit, reset and will try to resolve latter
                            chunksBuffer.readerIndex(lineStart);
                            break;
                        }
                    }
                }
            }
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

        private void newEvent(ChannelHandlerContext ctx, Map<String, HashMap<String, Object>> eventVars) {
            Journald.this.logger.trace("finishing event {}", eventVars);
            if (! eventVars.get(TRUSTEDFIELDS).isEmpty()) {
                Long timestamp = Optional.ofNullable(eventVars.get(TRUSTEDFIELDS).remove("source_realtime_timestamp")).map(Object::toString).map(Long::parseLong).orElse(null);
                Event e = Event.emptyEvent(getConnectionContext(ctx));
                e.put(USERDFIELDS, new HashMap<String, Object>(eventVars.get(USERDFIELDS)));
                e.put(TRUSTEDFIELDS, new HashMap<String, Object>(eventVars.get(TRUSTEDFIELDS)));
                if (timestamp != null) {
                    long seconds = Math.floorDiv(timestamp, (long)1e6);
                    long nano = (long)(timestamp % 1e6) * (long)1000;
                    e.setTimestamp(Instant.ofEpochSecond(seconds, nano));
                }
                eventVars.get(USERDFIELDS).clear();
                eventVars.get(TRUSTEDFIELDS).clear();
                events.add(e);
            }
        }

        /**
         * Returns the index in the buffer of the end of line found.
         * Returns -1 if no end of line was found in the buffer.
         */
        private int findEndOfLine(ByteBuf buffer) {
            int totalLength = buffer.readableBytes();
            int i = buffer.forEachByte(buffer.readerIndex(), totalLength, ByteProcessor.FIND_LF);
            return i - buffer.readerIndex();
        }

        private String readBinary(int size, CompositeByteBuf cbuf) {
            CharBuffer out = bufferolder.get().getCharBuffer(size);
            ByteBuffer in = bufferolder.get().getByteBuffer(size);
            cbuf.readBytes(in);
            CoderResult result = utf8decoder.get().reset().decode(in, out, true);
            if (result.isError()) {
                return null;
            } else {
                // It might be a casual string message, but with ANSI color code in it, remove them and keep the message
                String content = out.toString();
                Matcher withAnsi = ANSIPATTERN.matcher(content);
                if (withAnsi.find()) {
                    return withAnsi.replaceAll("");
                } else {
                    return content;
                }
            }
        }

    }

    public static class Builder extends AbstractHttp.Builder<Journald> {
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
        builder.setAggregatorSupplier(() -> new JournaldAgregator()).setReceiveHandler(new JournaldUploadHandler());
    }

    @Override
    public String getReceiverName() {
        return "Journald";
    }

}
