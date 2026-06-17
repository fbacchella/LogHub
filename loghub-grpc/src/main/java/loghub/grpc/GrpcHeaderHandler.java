package loghub.grpc;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.Descriptors.MethodDescriptor;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AsciiString;
import loghub.netty.HttpChannelConsumer;

@Sharable
public class GrpcHeaderHandler<S> extends SimpleChannelInboundHandler<Http2HeadersFrame> {

    private static final Logger logger = LogManager.getLogger();

    private static final AsciiString HEADER_CONTENT_TYPE = AsciiString.of("content-type");
    private static final AsciiString HEADER_TE = AsciiString.of("te");
    private static final AsciiString HEADER_TIMEOUT = AsciiString.of("grpc-timeout");
    private static final AsciiString GRPC_CONTENT_TYPE = AsciiString.of("application/grpc");
    private static final AsciiString POST = AsciiString.of("POST");
    private static final AsciiString TRAILERS = AsciiString.of("trailers");
    // Max possible timeout that can be expressed in the header
    private static final Duration MAXIMUM_TIMEOUT = Duration.ofHours(99_999_999L);

    private static final Pattern TIMEOUT_PATTERN = Pattern.compile("^(\\d+)([HMSmun])$");

    private final GrpcStats stats;
    private final Map<String, GrpcProcessor<S, ?, ?>> servicesByPath;
    private final Set<String> services;

    public GrpcHeaderHandler(GrpcStats stats, S server, GrpcProcessor<S, ?, ?>... providers) {
        this.stats = stats;
        Map<String, GrpcProcessor<S, ?, ?>> tmpCodecProvider = new HashMap<>();
        Set<String> tmpServices = new HashSet<>();
        for (GrpcProcessor<S, ?, ?> cp: providers) {
            cp.setServer(server);
            tmpServices.add(cp.getServiceName());
            BinaryCodec bc = cp.getProtobufCodec();
            bc.getMethods().forEach(e -> tmpCodecProvider.put(e.getKey(), cp));
        }
        this.servicesByPath = Map.copyOf(tmpCodecProvider);
        this.services = Set.copyOf(tmpServices);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http2HeadersFrame frame) {
        Http2Headers requestHeaders = frame.headers();
        String path = requestHeaders.path().toString();
        StringTokenizer st = new StringTokenizer(path, "/");
        String qualifiedMethodName;
        while (st.countTokens() > 2) {
            st.nextToken();
        }
        if (st.countTokens() == 2) {
            String serviceName = st.nextToken();
            if (!services.contains(serviceName)) {
                sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled service %s", serviceName), 200,
                        requestHeaders.path());
                return;
            }
            String methodName = st.nextToken();
            qualifiedMethodName = serviceName + "." + methodName;
        } else {
            sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Invalid gRPC path %s", requestHeaders.path()), 200,
                    requestHeaders.path());
            return;
        }

        if (! GRPC_CONTENT_TYPE.equals(requestHeaders.get(HEADER_CONTENT_TYPE))) {
            Http2Headers responseHeaders = new DefaultHttp2Headers().status("415");
            ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, true));
            return;
        } else if (! POST.contentEquals(requestHeaders.method())) {
            // a valid gRPC end point, but not a POST request
            sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Method should be POST, not %s", requestHeaders.method()), 200, requestHeaders.path());
            return;
        } else if (! servicesByPath.containsKey(qualifiedMethodName)) {
            sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled method %s", qualifiedMethodName), 200,
                    requestHeaders.path());
            return;
        } else if (! TRAILERS.contentEquals(requestHeaders.get(HEADER_TE))) {
            // Missing or invalid mandatory te header
            sendInitialHeaders(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Missing or invalid te header"), 200, requestHeaders.path());
            return;
        }
        GrpcProcessor<S, ?, ?> processor = servicesByPath.get(qualifiedMethodName);
        BinaryCodec codec = processor.getProtobufCodec();
        MethodDescriptor method = codec.getMethodDescriptor(qualifiedMethodName);
        Duration grpcTimout = parseTimeout(requestHeaders.get(HEADER_TIMEOUT));
        String grpcEncoding = Optional.ofNullable(requestHeaders.get("grpc-encoding")).orElse("").toString();
        logger.debug("Requested encoding will be {}", grpcEncoding);
        String encoding = switch (grpcEncoding) {
            case "gzip" -> CompressorStreamFactory.GZIP;
            case "identity", "" -> "";
            default -> null;
        };
        if (encoding == null) {
            // an unhandled encoding
            sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled encoding %s", grpcEncoding), 200, requestHeaders.path());
        } else if (method == null) {
            // an unhandled RPC
            sendInitialHeaders(ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Method %s has no registered RPC", qualifiedMethodName), 200, requestHeaders.path());
        } else {
            sendInitialHeaders(ctx, GrpcStatus.OK, 200, requestHeaders.path());
            GrpcStreamHandler<?, ?> streamHandler = GrpcStreamHandler.of(processor, requestHeaders, grpcTimout, qualifiedMethodName, ctx, stats, encoding);
            ctx.pipeline().addAfter(ctx.name(), "GrpcStreamHandler", streamHandler);
        }
    }

    @Override
    public boolean acceptInboundMessage(Object msg) {
        return msg instanceof Http2HeadersFrame;
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

    /**
     * Sends initial headers for a gRPC response. If it's not a valid gRPC request, it will be the end of the stream.
     * @param ctx ChannelHandlerContext for the current channel
     * @param status GrpcStatus indicating the status of the response
     * @param httpStatus HTTP status code to be used in the response
     */
    private void sendInitialHeaders(ChannelHandlerContext ctx, GrpcStatus status, int httpStatus, CharSequence path) {
        Http2Headers responseHeaders;
        if (status.isOk()) {
            assert httpStatus == 200 : "HTTP status must be 200 for successful gRPC requests";
            // It's a valid gRPC request, so more data will follow
            logger.debug("{} call", () -> path);
            responseHeaders = new DefaultHttp2Headers().status("200");
        } else {
            logger.warn("{} received invalid call with error status {}: {}",
                    () -> path, () -> httpStatus, status::getMessage);
            responseHeaders = status.getHeaders().status(Integer.toString(httpStatus));
        }
        responseHeaders.add(HEADER_CONTENT_TYPE, GRPC_CONTENT_TYPE)
                .add("grpc-encoding", "identity")
                .add("grpc-accept-encoding", "identity,gzip");
        ChannelFuture future = ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, ! status.isOk()));
        if (! status.isOk()) {
            future.addListener(f -> {
                Long startTime = ctx.channel().attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
                stats.stats(startTime, httpStatus, status);
            });
        }
    }

}
