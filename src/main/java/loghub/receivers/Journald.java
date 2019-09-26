package loghub.receivers;

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
import loghub.Event;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;

@Blocking
@SelfDecoder
public class Journald extends Http {

    private static final AttributeKey<Boolean> VALIDJOURNALD = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + Boolean.class.getName());
    private static final AttributeKey<List<Event>> EVENTS = AttributeKey.newInstance(Journald.class.getCanonicalName() + "." + List.class.getName());

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
     * This aggregator swallows valid journald events, that are sended as chunck by systemd-journal-upload
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
            super(8192);
            eventVars = new HashMap<>(2);
            eventVars.put(USERDFIELDS, new HashMap<String, Object>());
            eventVars.put(TRUSTEDFIELDS, new HashMap<String, Object>());
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
            try {
                boolean forward = true;
                if (isStartMessage(msg)) {
                    Journald.this.logger.debug("New journald post {}", msg);
                    HttpRequest headers = (HttpRequest) msg;
                    String contentType = Optional.ofNullable(headers.headers().get("Content-Type")).orElse("");
                    String uri = headers.uri().replaceAll("//", "/");
                    HttpMethod method = headers.method();
                    if ( ("application/vnd.fdo.journal".equals(contentType))
                                    &&  HttpMethod.POST.equals(method)
                                    && "/upload".equals(uri)) {
                        valid = true;
                    }
                    chunksBuffer = ctx.alloc().compositeBuffer();
                } else if (isContentMessage(msg) && valid) {
                    forward = false;
                    HttpContent chunk = (HttpContent) msg;
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
                                long size = -1;
                                if (chunksBuffer.readableBytes() > 8 ) {
                                    size = chunksBuffer.readLongLE();
                                }
                                if (size > 0 && chunksBuffer.readableBytes() > size) {
                                    byte[] content = new byte[(int) size];
                                    chunksBuffer.readBytes(content);
                                    // Read the EOL
                                    chunksBuffer.readByte();
                                    if (startKey == 2) {
                                        // fields starting with __ are privates, skip them, but after reading the binary part
                                        continue;
                                    }
                                    // It might be a casual string message, but with ANSI color code in it, remove them and keep the message
                                    if ("message".equals(key)) {
                                        Matcher withAnsi = ANSIPATTERN.matcher(new String(content, StandardCharsets.UTF_8));
                                        if (withAnsi.find()) {
                                            eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, withAnsi.replaceAll(""));
                                        } else {
                                            eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, content);
                                        }
                                    } else {
                                        eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, content);
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
                if (forward) {
                    super.decode(ctx, msg, out);
                }
            } catch (Exception e) {
                valid = false;
                ctx.channel().attr(VALIDJOURNALD).set(valid);
                events.clear();
                throw e;
            }
        }

        private void newEvent(ChannelHandlerContext ctx, Map<String, HashMap<String, Object>> eventVars) {
            Journald.this.logger.trace("finishing event {}", eventVars);
            if (! eventVars.get(TRUSTEDFIELDS).isEmpty()) {
                Long timestamp = Optional.ofNullable(eventVars.get(TRUSTEDFIELDS).remove("source_realtime_timestamp")).map(Object::toString).map(Long::parseLong).orElse(null);
                Event e = Event.emptyEvent(getConnectionContext(ctx, null));
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

    }

    @ContentType("text/plain; charset=utf-8")
    @RequestAccept(methods = {"GET", "PUT", "POST"})
    private class EventsHandler extends HttpRequestProcessing {
        @Override
        protected void processRequest(FullHttpRequest request,
                                      ChannelHandlerContext ctx)
                                                      throws HttpRequestFailure {
            if (Boolean.FALSE.equals(ctx.channel().attr(VALIDJOURNALD).get())) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Not a valid journald request");
            } else {
                ctx.channel().attr(EVENTS).get().forEach(Journald.this::send);
                ByteBuf body = request.content().alloc().buffer(4);
                body.writeCharSequence("OK.\n", StandardCharsets.UTF_8);
                writeResponse(ctx, request, HttpResponseStatus.ACCEPTED, body, 4);
            }
        }
    }

    @Override
    protected void settings(HttpReceiverServer.Builder builder) {
        super.settings(builder);
        builder.setAggregatorSupplier(() -> new JournaldAgregator()).setReceiveHandler(new EventsHandler());
    }

}
