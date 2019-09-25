package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
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
import loghub.Event;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;

@Blocking
public class Journald extends Http {

    private static final AttributeKey<Boolean> VALIDJOURNALD = AttributeKey.newInstance(Boolean.class.getName());

    private static final String TRUSTEDFIELDS = "fields_trusted";
    private static final String USERDFIELDS = "fields_user";
    private static final ByteProcessor FIND_EQUAL = new IndexOfProcessor((byte)'=');
    private static final ByteProcessor NON_UNDERSCORE = new ByteProcessor() {
        @Override
        public boolean process(byte value) throws Exception {
            return value == (byte) '_';
        }
    };

    private final Map<String, HashMap<String, String>> eventVars = new HashMap<>(2);
    private final ByteBuf prevChunck = Unpooled.buffer(4096);

    /**
     * This aggregator swallows valid journald events, that are sended as chunck by systemd-journal-upload
     * Other parts (the header) and non-valid requests are forwarded as-is, to be handled by the usual processing
     * @author Fabrice Bacchella
     *
     */
    class JournaldAgregator extends HttpObjectAggregator {

        public JournaldAgregator() {
            super(8192);
            eventVars.put(USERDFIELDS, new HashMap<String, String>());
            eventVars.put(TRUSTEDFIELDS, new HashMap<String, String>());
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
            boolean processed = false;
            boolean forward = true;
            if (Boolean.FALSE.equals(ctx.channel().attr(VALIDJOURNALD).get())) {
                processed = true;
            } else {
                if (msg instanceof HttpRequest) {
                    processed = true;
                    Journald.this.logger.debug("New journald post {}", msg);
                    HttpRequest headers = (HttpRequest) msg;
                    String contentType = Optional.ofNullable(headers.headers().get("Content-Type")).orElse("");
                    String uri = headers.uri().replaceAll("//", "/");
                    HttpMethod method = headers.method();
                    if ( (! "application/vnd.fdo.journal".equals(contentType))
                                    || !  HttpMethod.POST.equals(method)
                                    || ! "/upload".equals(uri)) {
                        ctx.channel().attr(VALIDJOURNALD).set(false);
                    } else {
                        ctx.channel().attr(VALIDJOURNALD).set(true);
                    }
                } else if (isContentMessage(msg)) {
                    processed = true;
                    forward = false;
                    HttpContent chunk = (HttpContent) msg;
                    Journald.this.logger.trace("New journald chunk of events, length {}", () -> chunk.content().readableBytes());

                    ByteBuf chunckContent = chunk.content();
                    int eolPos;
                    while ((eolPos = findEndOfLine(chunckContent)) >= 0) {
                        ByteBuf lineBuffer;
                        if (prevChunck.readableBytes() > 0) {
                            // Some data from previous chunck not yet handled
                            // Copy the current line after them and process it
                            chunckContent.readBytes(prevChunck, eolPos);
                            lineBuffer = prevChunck.readSlice(prevChunck.readableBytes());
                            prevChunck.clear();
                        } else {
                            lineBuffer = chunckContent.readSlice(eolPos);
                        }
                        // Read the EOL
                        chunckContent.readByte();
                        if (eolPos == 0) {
                            // An empty line, event separator
                            newEvent(ctx, eventVars);
                        } else {
                            // Fields are extracted in place, to avoid many useless strings copy
                            int equalPos = lineBuffer.forEachByte(FIND_EQUAL);
                            if (equalPos > 0) {
                                ByteBuf keyBuffer = lineBuffer.readSlice(equalPos);
                                // Used to detect the number of _ in front of a field name
                                // 1, it's a trusted field, managed by journald
                                // 2, it's a private field, probably to be dropped
                                // Fields are explained at https://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
                                int startKey = keyBuffer.forEachByte(NON_UNDERSCORE);
                                if (startKey == 2) {
                                    // fields starting with __ are privates, skip them
                                    continue;
                                }
                                keyBuffer.readerIndex(startKey);
                                String key = keyBuffer.toString(StandardCharsets.UTF_8).toLowerCase(Locale.ENGLISH);

                                boolean userField = startKey == 0;
                                lineBuffer.readerIndex(equalPos + 1);
                                String value = lineBuffer.toString(StandardCharsets.UTF_8);
                                eventVars.get(userField ? USERDFIELDS : TRUSTEDFIELDS).put(key, value);
                            } else {
                                newEvent(ctx, eventVars);
                            }
                        }
                    }
                    if (chunckContent.readableBytes() > 0) {
                        //Save the unused chunck bytes
                        chunckContent.getBytes(chunckContent.readerIndex(), prevChunck, chunckContent.readableBytes());
                    } else {
                        prevChunck.clear();
                    }
                    if (isLastContentMessage(chunk)) {
                        super.decode(ctx, LastHttpContent.EMPTY_LAST_CONTENT, out);
                    }
                }
            }
            if (! processed) {
                Journald.this.logger.warn("Unexpected message part {}", msg);
            }
            if (forward) {
                super.decode(ctx, msg, out);
            }
        }

        private void newEvent(ChannelHandlerContext ctx, Map<String, HashMap<String, String>> eventVars) {
            Journald.this.logger.trace("finishing event {}", eventVars);
            if (! eventVars.get(TRUSTEDFIELDS).isEmpty()) {
                Long timestamp = Optional.ofNullable(eventVars.get(TRUSTEDFIELDS).remove("source_realtime_timestamp")).map(Long::parseLong).orElse(null);
                Event e = Event.emptyEvent(getConnectionContext(ctx, null));
                e.put(USERDFIELDS, new HashMap<String, String>(eventVars.get(USERDFIELDS)));
                e.put(TRUSTEDFIELDS, new HashMap<String, String>(eventVars.get(TRUSTEDFIELDS)));
                if (timestamp != null) {
                    long seconds = Math.floorDiv(timestamp, (long)1e6);
                    long nano = (long)(timestamp % 1e6) * (long)1000;
                    e.setTimestamp(Instant.ofEpochSecond(seconds, nano));
                }
                eventVars.get(USERDFIELDS).clear();
                eventVars.get(TRUSTEDFIELDS).clear();
                send(e);
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
    private class FailedHandler extends HttpRequestProcessing {
        @Override
        protected void processRequest(FullHttpRequest request,
                                      ChannelHandlerContext ctx)
                                                      throws HttpRequestFailure {
            if (Boolean.FALSE.equals(ctx.channel().attr(VALIDJOURNALD).get())) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Not a valid journald request");
            } else {
                ByteBuf body = request.content().alloc().buffer(4);
                body.writeCharSequence("OK.\n", StandardCharsets.UTF_8);
                writeResponse(ctx, request, HttpResponseStatus.ACCEPTED, body, 4);
            }
        }
    }

    @Override
    protected void settings(HttpReceiverServer.Builder builder) {
        super.settings(builder);
        builder.setAggregatorSupplier(() -> new JournaldAgregator()).setReceiveHandler(new FailedHandler()).setAuthHandler(null);
    }

}
