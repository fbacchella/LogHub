package loghub.decoders;

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
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import io.netty.util.ByteProcessor.IndexOfProcessor;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Event;
import lombok.Data;

@BuilderClass(JournaldExport.Builder.class)
public class JournaldExport extends Decoder {

    public static class Builder extends Decoder.Builder<JournaldExport> {
        @Override
        public JournaldExport build() {
            return new JournaldExport(this);
        }
    };

    public static Builder getBuilder() {
        return new Builder();
    }

    private static final String TRUSTEDFIELDS = "fields_trusted";
    private static final String USERDFIELDS = "fields_user";
    private static final ByteProcessor FIND_EQUAL = new IndexOfProcessor((byte)'=');
    private static final ByteProcessor NON_UNDERSCORE = new ByteProcessor() {
        @Override
        public boolean process(byte value) throws Exception {
            return value == (byte) '_';
        }
    };

    @Data
    private static class EventVars {
        final HashMap<String, Object> userFields = new HashMap<String, Object>();
        final HashMap<String, Object> trustedFields = new HashMap<String, Object>();
        Map<String, Object> get(boolean userField) {
            return userField ? userFields : trustedFields;
        }
        void clear() {
            userFields.clear();
            trustedFields.clear();
        }
    }

    private static final Pattern ANSIPATTERN = Pattern.compile("\u001B\\[[;\\d]*[ -/]*[@-~]");

    private static final ThreadLocal<CharsetDecoder> utf8decoder = ThreadLocal.withInitial( () -> {
        return StandardCharsets.UTF_8.newDecoder().onUnmappableCharacter(CodingErrorAction.REPORT).onMalformedInput(CodingErrorAction.REPORT);
    });

    private static final ThreadLocal<EventVars> threadEventVars = ThreadLocal.withInitial( () -> {
        return new EventVars();
    });

    protected JournaldExport(Builder builder) {
        super(builder);
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException {
        ByteBuf buffer = Unpooled.wrappedBuffer(msg, offset, length);
        return decodeObject(connectionContext, buffer);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf chunksBuffer) throws DecodeException {
        List<Event> events = new ArrayList<Event>();
        EventVars eventVars = threadEventVars.get();
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
                Optional.ofNullable(newEvent(ctx, eventVars)).ifPresent(events::add);
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

                    if (startKey == 2 && ! "realtime_timestamp".equals(key)) {
                        // fields starting with __ are privates, skip them
                        // but we keep __REALTIME_TIMESTAMP, as _SOURCE_REALTIME_TIMESTAMP is
                        // not always present
                        continue;
                    }
                    // A equal was found, a simple textual field
                    lineBuffer.readerIndex(equalPos + 1); // Skip the '='
                    String value = lineBuffer.toString(StandardCharsets.UTF_8);
                    eventVars.get(userField).put(key, value);
                } else {
                    // A binary field
                    int size = -1;
                    if (chunksBuffer.readableBytes() > 8) {
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
                            // size includes the final LF
                            Object value = readBinary(size, chunksBuffer);
                            // Read the EOL
                            chunksBuffer.readByte();
                            eventVars.get(userField).put(key, value);
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
        return events;
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

    private Object readBinary(int size, ByteBuf cbuf) {
        CharBuffer out = CharBuffer.allocate(size);
        ByteBuffer in = ByteBuffer.allocate(size);
        cbuf.readBytes(in);
        in.flip();
        CoderResult result = utf8decoder.get().reset().decode(in, out, true);
        out.flip();
        if (result.isError()) {
            return in.asReadOnlyBuffer();
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

    private Event newEvent(ConnectionContext<?> ctx, EventVars eventVars) {
        if (! eventVars.trustedFields.isEmpty()) {
            Event e = Event.emptyEvent(ctx);
            String timestampString = (String) Optional.ofNullable(eventVars.trustedFields.remove("source_realtime_timestamp"))
                                                      .orElse(eventVars.trustedFields.remove("realtime_timestamp"));
            // Ensure that realtime_timestamp is removed anyway
            eventVars.trustedFields.remove("realtime_timestamp");
            if (timestampString != null) {
                long timestamp = Long.parseLong(timestampString);
                long seconds = Math.floorDiv(timestamp, (long)1e6);
                long nano = (timestamp % (long)1e6) * 1000L;
                e.setTimestamp(Instant.ofEpochSecond(seconds, nano));
            }
            e.put(USERDFIELDS, new HashMap<String, Object>(eventVars.userFields));
            e.put(TRUSTEDFIELDS, new HashMap<String, Object>(eventVars.trustedFields));
            eventVars.clear();
            return e;
        } else {
           logger.warn("No journald event {}", eventVars);
           return null;
        }
    }

}
