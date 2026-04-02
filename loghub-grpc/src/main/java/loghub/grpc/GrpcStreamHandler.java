package loghub.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
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
import loghub.metrics.Stats;
import loghub.netty.HttpChannelConsumer;
import loghub.receivers.Receiver;
import lombok.Getter;

public class GrpcStreamHandler<I, O> extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LogManager.getLogger();

    public static class Factory {
        private final Map<String, BinaryCodec> servicesByPath;
        private final Object statsHolder;
        private Map<String, MethodProcessor<?, ?>> transformers = new HashMap<>();
        @SafeVarargs
        public Factory(Object statsHolder, BinaryCodec... services) {
            Map<String, BinaryCodec> tmpMap = new HashMap<>();
            for (BinaryCodec bc: services) {
                bc.getMethods().forEach(e -> tmpMap.put(e.getKey(), bc));
            }
            servicesByPath = Map.copyOf(tmpMap);
            this.statsHolder = statsHolder;
        }
        public void register(String methodQualifiedName, MethodProcessor<?, ?> transformer) {
            if (transformer != null) {
                transformers.put(methodQualifiedName, transformer);
            } else {
                transformers.remove(methodQualifiedName);
            }
        }
        public synchronized <I, O> GrpcStreamHandler<I, O> get() {
            // Lock the transformers. The map is immutable after this point.
            // The current implementation returns the input if it's already an immutable map
            transformers = Map.copyOf(transformers);
            return new GrpcStreamHandler<>(statsHolder, servicesByPath, transformers);
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
    // Max possible timeout that can be expressed in the header
    private static final Duration MAXIMUM_TIMEOUT = Duration.ofHours(99_999_999L);

    private static final Pattern TIMEOUT_PATTERN = Pattern.compile("^(\\d+)([HMSmun])$");

    private final Map<String, BinaryCodec> servicesByPath;
    private final Map<String, MethodProcessor<?, ?>> transformers;
    private final Object statsHolder;
    private ByteBuf currentMessage;
    private String encoding;

    @Getter
    private String qualifiedMethodName;
    @Getter
    private BinaryCodec codec;
    @Getter
    private Duration grpcTimout;
    @Getter
    private ChannelHandlerContext currentContext;
    @Getter
    private Http2Headers requestHeaders = EmptyHttp2Headers.INSTANCE;
    @Getter
    private MethodDescriptor method;
    @Getter
    MethodProcessor<I, O> methodConsumer;
    private boolean canProcessData = false;

    private GrpcStreamHandler(Object statsHolder, Map<String, BinaryCodec> servicesByPath, Map<String, MethodProcessor<?, ?>> transformers) {
        this.statsHolder = statsHolder;
        this.servicesByPath = servicesByPath;
        this.transformers = transformers;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        switch (msg) {
        case Http2HeadersFrame hf -> onHeadersReceived(ctx, hf);
        case Http2DataFrame df when canProcessData -> onDataReceived(ctx, df);
        case Http2DataFrame df -> logger.debug("Skipped invalid data {}", df);
        default -> ctx.fireChannelRead(msg);
        }
    }

    private void onHeadersReceived(ChannelHandlerContext ctx,
            Http2HeadersFrame frame) {
        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }
        requestHeaders = readOnlyHeaders(frame.headers());
        logger.debug("Headers frame {}", requestHeaders);
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
            if (codec != null) {
                method = codec.getMethodDescriptor(qualifiedMethodName);
            }
            methodConsumer = (MethodProcessor<I, O>) transformers.get(qualifiedMethodName);

            grpcTimout = parseTimeout(requestHeaders.get(HEADER_TIMEOUT));
            String grpcEncoding = Optional.ofNullable(requestHeaders.get("grpc-encoding")).orElse("").toString();
            logger.debug("Requested encoding will be {}", grpcEncoding);
            encoding = switch (grpcEncoding) {
                case "gzip" -> CompressorStreamFactory.GZIP;
                case "identity" -> "";
                default -> grpcEncoding;
            };
            if (! GRPC_CONTENT_TYPE.equals(requestHeaders.get(HEADER_CONTENT_TYPE)) && codec == null) {
                // Not a gRPC request, skip it
                ctx.fireChannelRead(frame);
            } else if (codec == null) {
                // an unhandled gRPC end point
                sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled method %s", qualifiedMethodName), 200);
            } else if (encoding == null) {
                // an unhandled encoding
                sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled encoding %s", grpcEncoding), 200);
            } else if (methodConsumer == null || method == null) {
                // an unhandled RPC
                sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Method %s has no registered RPC", qualifiedMethodName), 200);
            } else if (! POST.contentEquals(requestHeaders.method())) {
                // a valid gRPC end point, but not a POST request
                sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Method should be POST, not %s", requestHeaders.method()), 400);
            } else if (! TRAILERS.contentEquals(requestHeaders.get(HEADER_TE))) {
                // Missing or invalid mandatory te header
                sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Missing or invalid te header", requestHeaders.method()), 400);
            } else {
                canProcessData = true;
                sendInitialHeaders(ctx, GrpcStatus.OK, 200);
            }
        } else {
            ctx.fireChannelRead(frame);
        }
    }

    private Http2Headers readOnlyHeaders(Http2Headers sourceHeaders) {
        AsciiString methodName = AsciiString.of(sourceHeaders.method());
        AsciiString path       = AsciiString.of(sourceHeaders.path());
        AsciiString scheme     = AsciiString.of(sourceHeaders.scheme());
        AsciiString authority  = AsciiString.of(sourceHeaders.authority());

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
                methodName,
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
    private Duration parseTimeout(CharSequence grpcTimeout) {
        if (grpcTimeout == null || grpcTimeout.isEmpty()) {
            return Duration.ofNanos(Long.MAX_VALUE);
        }

        Matcher matcher = TIMEOUT_PATTERN.matcher(grpcTimeout);
        if (!matcher.matches()) {
            logger.warn("Invalid grpc-timeout format: {}", grpcTimeout);
            return MAXIMUM_TIMEOUT;
        }

        long value = Long.parseLong(matcher.group(1));
        char unit  = matcher.group(2).charAt(0);

        if (value > 99_999_999L) {
            logger.warn("grpc-timeout exceeds maximum allowed value (99999999): {}", value);
            return MAXIMUM_TIMEOUT;
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
                yield MAXIMUM_TIMEOUT;
            }
        };
    }

    private void onDataReceived(ChannelHandlerContext ctx, Http2DataFrame frame) {
        logger.debug("Data frame {}", frame);
        try {
            List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();

            ByteBuf buf = frame.content();
            if (currentMessage == null) {
                currentMessage = ctx.alloc().buffer(buf.readableBytes());
            }
            currentMessage.writeBytes(buf);
            // --- gRPC wire format -------------------------------------------
            // Byte 0    : compressed-flag (0 = not compressed, 1 = compressed)
            // Bytes 1-4 : message length (big-endian unsigned 32-bit integer)
            // Bytes 5+  : serialized protobuf payload
            // ----------------------------------------------------------------

            // Loop over messages in the data frame
            while (currentMessage.readableBytes() >= GRPC_FRAME_HEADER_LENGTH) {
                currentMessage.markReaderIndex();
                byte compressionFlag = currentMessage.readByte();
                int messageLength = currentMessage.readInt();
                boolean compressed = compressionFlag != GRPC_FRAME_NOT_COMPRESSED;
                if (currentMessage.readableBytes() < messageLength ) {
                    logger.trace("Not yet complet gRPC message, expected {}, got {}", messageLength, currentMessage.readableBytes());
                    currentMessage.resetReaderIndex();
                    break;
                }
                logger.debug("Receveid gRPC message, compression flag = {}, messageLength = {}", compressionFlag, messageLength);
                if (statsHolder instanceof Receiver r) {
                    Stats.getMetric(r, "gRPCMessage", Histogram.class).update(currentMessage.readableBytes());
                }
                ByteBuf messageBuf = decompress(compressed, currentMessage.readSlice(messageLength));
                I request = codec.decode(CodedInputStream.newInstance(messageBuf.nioBuffer()), method.getInputType(), unknownFields);
                currentContext = ctx;
                O processed = methodConsumer.doProcessing(this, request);
                byte[] response = switch (processed) {
                case byte[] r ->  r;
                case Map<?, ?> m -> {
                    Descriptor omd = method.getOutputType();
                    yield codec.encode(omd, (Map<String, Object>) m).toByteArray();
                }
                default -> throw new IllegalArgumentException(
                        "Method " + qualifiedMethodName + " returned unexpected type: " + processed.getClass()
                                                                                                   .getName());
                };
                writeGrpcDataFrame(ctx, response);
            }

            if (frame.isEndStream()) {
                if (currentMessage.readableBytes() > 0) {
                    logger.trace("Discarded bytes from HTTP/2 frame: {}", currentMessage.readableBytes());
                }
                sendTrailers(ctx, GrpcStatus.OK);
            }
        } catch (IOException ex) {
            sendTrailers(ctx, GrpcStatus.FAILED_PRECONDITION.withMessage("Invalid input", ex));
        } catch (RuntimeException ex) {
            logger.atError().withThrowable(ex).log("{}: {}", () -> requestHeaders.path(), () -> Helpers.resolveThrowableException(ex));
            sendTrailers(ctx, GrpcStatus.INTERNAL.withMessage("Internal error processing gRPC request"));
        } catch (GrpcMethodException e) {
            sendTrailers(ctx, e.getStatus());
        } finally {
            frame.release();
        }
    }

    private ByteBuf decompress(boolean compressed, ByteBuf messageBuf) throws IOException {
        if (! compressed || encoding.isEmpty()) {
            return messageBuf;
        } else {
            ByteBuf out = Unpooled.compositeBuffer(messageBuf.readableBytes());
            try (InputStream ins = CompressorStreamFactory.getSingleton().createCompressorInputStream(
                    encoding, new ByteBufInputStream(messageBuf));
                    OutputStream outs = new ByteBufOutputStream(out)) {
                IOUtils.copy(ins, outs);
                return out;
            } catch (IOException e) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Unable to decompress gRPC message: {}", Helpers.resolveThrowableException(e));
                throw e;
            }
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
        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }

        if (status.isOk()) {
            logger.debug("{} done", () -> requestHeaders.path());
        } else {
            logger.warn("{} failed with error status: {}", () -> requestHeaders.path(), () -> status);
        }
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(status.getHeaders(), true)).addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("Failed to send trailers: {}", f.cause().getMessage());
            }
            Long startTime = ctx.channel().attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
            stats(startTime, 200, status);
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
                    () -> requestHeaders.path(), () -> httpStatus, status::getMessage);
            responseHeaders = status.getHeaders().status(Integer.toString(httpStatus));
        }
        responseHeaders.add(HEADER_CONTENT_TYPE, GRPC_CONTENT_TYPE)
                       .add("grpc-encoding", "identity")
                       .add("grpc-accept-encoding", "identity,gzip");
        ChannelFuture future = ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, ! status.isOk()));
        if (! status.isOk()) {
            future.addListener(f -> {
                Long startTime = ctx.channel().attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
                stats(startTime, httpStatus, status);
            });
        }
    }

    private void stats(Long startTime , int httpStatus, GrpcStatus grepStatus) {
        long duration = startTime != null ? System.nanoTime() - startTime : 0;
        Stats.getWebMetric(statsHolder, httpStatus).update(duration, TimeUnit.NANOSECONDS);
        Stats.getMetric(statsHolder, "gRPC." + grepStatus.resolveKey(), Timer.class).update(duration, TimeUnit.NANOSECONDS);
    }

}
