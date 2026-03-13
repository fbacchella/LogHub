package loghub.grpc;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.EmptyHttp2Headers;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.ReadOnlyHttp2Headers;
import io.netty.util.AsciiString;
import loghub.Helpers;
import lombok.Getter;

public class GrpcStreamHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LogManager.getLogger();

    public static class Factory {
        private final Map<String, BinaryCodec<GrpcStreamHandler>> servicesByPath;
        private Map<String, BiFunction<?, GrpcStreamHandler, ?>> transformers = new HashMap<>();
        @SafeVarargs
        public Factory(BinaryCodec<GrpcStreamHandler>... services) {
            Map<String, BinaryCodec<GrpcStreamHandler>> tmpMap = new HashMap<>();
            for (BinaryCodec<GrpcStreamHandler> bc: services) {
                bc.getMethods().forEach(e -> tmpMap.put(e.getKey(), bc));
            }
            servicesByPath = Map.copyOf(tmpMap);
        }
        public void register(String methodQualifiedName, BiFunction<?, GrpcStreamHandler, ?> transformer) {
            if (transformer != null) {
                transformers.put(methodQualifiedName, transformer);
            } else {
                transformers.remove(methodQualifiedName);
            }
        }
        public synchronized GrpcStreamHandler get() {
            // Lock the transformers. The map is immutable after this point.
            // The current implementation returns the input if it's already an immutable map
            transformers = Map.copyOf(transformers);
            return new GrpcStreamHandler(servicesByPath, transformers);
        }
    }

    // gRPC length-prefixed message framing constants
    private static final int GRPC_FRAME_HEADER_LENGTH = 5; // 1 byte flag + 4 bytes length
    private static final byte GRPC_FRAME_NOT_COMPRESSED = 0x00;
    private static final AsciiString HEADER_CONTENT_TYPE = AsciiString.of("content-type");
    private static final AsciiString HEADER_TE = AsciiString.of("te");
    private static final AsciiString HEADER_TIMEOUT = AsciiString.of("grpc-timeout");
    private static final AsciiString GRPC_CONTENT_TYPE = AsciiString.of("application/grpc");
    private static final AsciiString POST = AsciiString.of("POST");
    private static final AsciiString TRAILERS = AsciiString.of("trailers");

    private static final Pattern TIMEOUT_PATTERN = Pattern.compile("^(\\d+)([HMSmun])$");

    private final Map<String, BinaryCodec<GrpcStreamHandler>> servicesByPath;
    private final Map<String, BiFunction<?, GrpcStreamHandler, ?>> transformers;

    @Getter
    private String qualifiedMethodName;
    @Getter
    private BinaryCodec<GrpcStreamHandler> codec;
    @Getter
    private Duration grpcTimout;
    @Getter
    private ChannelHandlerContext currentContext;
    @Getter
    Http2Headers requestHeaders = EmptyHttp2Headers.INSTANCE;

    private GrpcStreamHandler(Map<String, BinaryCodec<GrpcStreamHandler>> servicesByPath, Map<String, BiFunction<?, GrpcStreamHandler, ?>> transformers) {
        this.servicesByPath = servicesByPath;
        this.transformers = transformers;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        switch (msg) {
        case Http2HeadersFrame hf -> onHeadersReceived(ctx, hf);
        case Http2DataFrame df when codec != null -> onDataReceived(ctx, df);
        default -> ctx.fireChannelRead(msg);
        }
    }

    private void onHeadersReceived(ChannelHandlerContext ctx,
            Http2HeadersFrame frame) {
        requestHeaders = readOnlyHeaders(frame.headers());
        String path = requestHeaders.path().toString();
        StringTokenizer st = new StringTokenizer(path, "/");
        while (st.countTokens() > 2) {
            st.nextToken();
        }
        if (st.countTokens() == 2) {
            String serviceName = st.nextToken();
            String methodName = st.nextToken();
            qualifiedMethodName = serviceName + "." + methodName;
            codec = servicesByPath.get(qualifiedMethodName);
            grpcTimout = parse(requestHeaders.get(HEADER_TIMEOUT));
            if (! GRPC_CONTENT_TYPE.equals(requestHeaders.get(HEADER_CONTENT_TYPE)) && codec == null) {
                // Not a gRPC request, skip it
                ctx.fireChannelRead(frame);
            } else if (grpcTimout == null) {
                // Unparsable timeout header
                sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Missing timeout header", requestHeaders.method()), 400);
            } else if (codec == null) {
                // an unhandled gRPC end point
                sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled method %s", qualifiedMethodName), 200);
            } else if (! POST.contentEquals(requestHeaders.method())) {
                // a valid gRPC end point, but not a POST request
                sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Method should be POST, not %s", requestHeaders.method()), 400);
            } else if (! TRAILERS.contentEquals(requestHeaders.get(HEADER_TE))) {
                // Missing or invalid mandatory te header
                sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Missing or invalid te header", requestHeaders.method()), 400);
            } else {
                sendInitialHeaders(ctx, GrpcStatus.OK, 200);
            }
        } else {
            ctx.fireChannelRead(frame);
        }
    }

    private Http2Headers readOnlyHeaders(Http2Headers sourceHeaders) {
        // Extraction des pseudo-en-têtes obligatoires
        AsciiString method    = AsciiString.of(sourceHeaders.method());
        AsciiString path      = AsciiString.of(sourceHeaders.path());
        AsciiString scheme    = AsciiString.of(sourceHeaders.scheme());
        AsciiString authority = AsciiString.of(sourceHeaders.authority());

        // Collecte des en-têtes ordinaires (non pseudo)
        List<AsciiString> others = new ArrayList<>();
        for (Map.Entry<CharSequence, CharSequence> entry : sourceHeaders) {
            CharSequence name = entry.getKey();
            if (!name.isEmpty() && name.charAt(0) != ':') {
                others.add(AsciiString.of(name));
                others.add(AsciiString.of(entry.getValue()));
            }
        }

        return ReadOnlyHttp2Headers.clientHeaders(
                false,
                method,
                path,
                scheme,
                authority,
                others.toArray(new AsciiString[0])
        );
    }

    /**
     * Parses a grpc-timeout header value as defined by the gRPC specification
     * (e.g. "1S", "500m", "100u") into a {@link Duration}.
     *
     * @param grpcTimeout raw value of the grpc-timeout header
     * @return the corresponding {@link Duration}
     * @throws IllegalArgumentException if the format is invalid
     */
    private Duration parse(CharSequence grpcTimeout) {
        if (grpcTimeout == null || grpcTimeout.isEmpty()) {
            return Duration.ofNanos(Long.MAX_VALUE);
        }

        Matcher matcher = TIMEOUT_PATTERN.matcher(grpcTimeout);
        if (!matcher.matches()) {
            logger.warn("Invalid grpc-timeout format: {}", grpcTimeout);
            return null;
        }

        long value = Long.parseLong(matcher.group(1));
        char unit  = matcher.group(2).charAt(0);

        if (value > 99_999_999L) {
            logger.warn("grpc-timeout exceeds maximum allowed value (99999999): {}", value);
            return null;
        }

        return switch (unit) {
            case 'H' -> Duration.ofHours(value);
            case 'M' -> Duration.ofMinutes(value);
            case 'S' -> Duration.ofSeconds(value);
            case 'm' -> Duration.ofMillis(value);
            case 'u' -> Duration.ofNanos(value * 1_000L);
            case 'n' -> Duration.ofNanos(value);
            default  -> {
                logger.warn("Unknown grpc-timeout unit: {}", unit);
                yield null;
            }
        };
    }

    private void onDataReceived(ChannelHandlerContext ctx, Http2DataFrame frame) {
        try {
            List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();

            ByteBuf buf = frame.content();
            MethodDescriptor md = codec.getMethodDescriptor(qualifiedMethodName);
            // --- gRPC wire format -------------------------------------------
            // Byte 0    : compressed-flag (0 = not compressed, 1 = compressed)
            // Bytes 1-4 : message length (big-endian unsigned 32-bit integer)
            // Bytes 5+  : serialized protobuf payload
            // ----------------------------------------------------------------

            if (buf.readableBytes() < GRPC_FRAME_HEADER_LENGTH) {
                sendTrailers(ctx, GrpcStatus.INTERNAL.withMessage("Incomplete gRPC frame header"));
                return;
            }

            byte compressionFlag = buf.readByte();
            int  messageLength   = buf.readInt();

            if (compressionFlag != GRPC_FRAME_NOT_COMPRESSED) {
                sendTrailers(ctx,
                        GrpcStatus.UNIMPLEMENTED.withMessage("Compressed messages are not supported by this handler"));
                return;
            }

            if (buf.readableBytes() < messageLength) {
                sendTrailers(ctx,
                        GrpcStatus.INTERNAL.withMessage("Declared message length %d exceeds available bytes %d", messageLength, buf.readableBytes()));
                return;
            }
            Object request = codec.decode(CodedInputStream.newInstance(buf.nioBuffer()), md.getInputType(), unknownFields);
            BiFunction<?, GrpcStreamHandler, ?> methodConsumer = transformers.get(qualifiedMethodName);
            if (methodConsumer == null) {
                sendTrailers(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Method %s has no registered transformer", qualifiedMethodName));
                return;
            }
            currentContext = ctx;
            byte[] response = codec.process(qualifiedMethodName, this, request, methodConsumer);
            writeGrpcDataFrame(ctx, response);
            sendTrailers(ctx, GrpcStatus.OK);
        } catch (IOException ex) {
            sendTrailers(ctx, GrpcStatus.FAILED_PRECONDITION.withMessage("Invalid input", ex));
        } catch (RuntimeException ex) {
            logger.atError().withThrowable(ex).log("{}: {}", () -> requestHeaders.path(), () -> Helpers.resolveThrowableException(ex));
            sendTrailers(ctx, GrpcStatus.INTERNAL.withMessage("Internal error processing gRPC request"));
        } finally {
            frame.release();
        }
    }

    void writeGrpcDataFrame(ChannelHandlerContext ctx, byte[] responseBytes) {
        int totalLength = GRPC_FRAME_HEADER_LENGTH + responseBytes.length;
        ByteBuf buf = ctx.alloc().buffer(totalLength);

        buf.writeByte(GRPC_FRAME_NOT_COMPRESSED); // compression flag
        buf.writeInt(responseBytes.length);        // message length (4 bytes, big-endian)
        buf.writeBytes(responseBytes);             // serialized protobuf payload

        ctx.writeAndFlush(new DefaultHttp2DataFrame(buf, false));
    }

    private void sendTrailers(ChannelHandlerContext ctx, GrpcStatus status) {
        if (status.isOk()) {
            logger.debug("{} done", () -> requestHeaders.path());
        } else {
            logger.warn("{} failed with error status: {}", () -> requestHeaders.path(), () -> status);
        }
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(status.getHeaders(), true)).addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("Failed to send trailers: {}", f.cause().getMessage());
            }
        });
    }

    /**
     * Sends initial headers for a gRPC response. If it's not a valid gRPC request, it will be the end of the stream.
     * @param ctx ChannelHandlerContext for the current channel
     * @param status GrpcStatus indicating the status of the response
     * @param httpStatus HTTP status code to be used in the response
     */
    private void sendInitialHeaders(ChannelHandlerContext ctx, GrpcStatus status, int httpStatus) {
        Http2Headers responseHeaders;
        if (status.isOk()) {
            assert httpStatus == 200 : "HTTP status must be 200 for successful gRPC requests";
            // It's a valid gRPC request, so more data will follow
            logger.debug("{} call", () -> requestHeaders.path());
            responseHeaders = new DefaultHttp2Headers().status("200");
        } else {
            logger.warn("{} received invalid call with error status {}: {}",
                    () -> requestHeaders.path(), () -> httpStatus, status::getStatus);
            responseHeaders = status.getHeaders().status(Integer.toString(httpStatus));
        }
        responseHeaders.add(HEADER_CONTENT_TYPE, GRPC_CONTENT_TYPE)
                       .add("grpc-encoding", "identity")
                       .add("grpc-accept-encoding", "identity,gzip");
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, ! status.isOk()));
    }

}
