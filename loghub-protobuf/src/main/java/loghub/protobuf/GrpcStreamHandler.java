package loghub.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.BiFunction;

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
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AsciiString;

public class GrpcStreamHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LogManager.getLogger();

    public static class Factory {
        private final Map<String, BinaryCodec<ChannelHandlerContext>> servicesByPath;
        private Map<String, BiFunction<?, ChannelHandlerContext, ?>> transformers = new HashMap<>();
        @SafeVarargs
        public Factory(BinaryCodec<ChannelHandlerContext>... services) {
            Map<String, BinaryCodec<ChannelHandlerContext>> tmpMap = new HashMap<>();
            for (BinaryCodec<ChannelHandlerContext> bc: services) {
                bc.getMethods().forEach(e -> tmpMap.put(e.getKey(), bc));
            }
            servicesByPath = Map.copyOf(tmpMap);
        }
        public void register(String methodQualifiedName, BiFunction<?, ChannelHandlerContext, ?> transformer) {
            if (transformer != null) {
                transformers.put(methodQualifiedName, transformer);
            } else {
                transformers.remove(methodQualifiedName);
            }
        }
        public synchronized GrpcStreamHandler get() {
            // Lock the transformers. The map is immutable after this point.
            // The current implementation resute the inpput if it's alread an immutable map
            transformers = Map.copyOf(transformers);
            return new GrpcStreamHandler(servicesByPath, transformers);
        }
    }

    // gRPC length-prefixed message framing constants
    private static final int GRPC_FRAME_HEADER_LENGTH = 5; // 1 byte flag + 4 bytes length
    private static final byte GRPC_FRAME_NOT_COMPRESSED = 0x00;
    private static final AsciiString HEADER_CONTENT_TYPE = AsciiString.of("content-type");
    private static final AsciiString GRPC_CONTENT_TYPE = AsciiString.of("application/grpc");
    private static final AsciiString POST = AsciiString.of("POST");

    private final Map<String, BinaryCodec<ChannelHandlerContext>> servicesByPath;
    private final Map<String, BiFunction<?, ChannelHandlerContext, ?>> transformers;

    private String qualifiedMethodName;
    private BinaryCodec<ChannelHandlerContext> codec;

    private GrpcStreamHandler(Map<String, BinaryCodec<ChannelHandlerContext>> servicesByPath, Map<String, BiFunction<?, ChannelHandlerContext, ?>> transformers) {
        this.servicesByPath = servicesByPath;
        this.transformers = transformers;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Http2HeadersFrame hf) {
            onHeadersReceived(ctx, hf);
        } else if (codec != null && msg instanceof Http2DataFrame df) {
            onDataReceived(ctx, df);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void onHeadersReceived(ChannelHandlerContext ctx,
            Http2HeadersFrame frame) {
        Http2Headers requestHeaders = frame.headers();
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
        }

        if (codec != null && ! POST.equals(requestHeaders.method())) {
            sendTrailers(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Method should be POST, not %s", requestHeaders.method()));
        } else if (codec != null && ! requestHeaders.get(HEADER_CONTENT_TYPE).equals(GRPC_CONTENT_TYPE)) {
            sendTrailers(ctx, GrpcStatus.INVALID_ARGUMENT.withMessage("Content-Type should be application/grpc, not %s", requestHeaders.get(HEADER_CONTENT_TYPE)));
        } else if (codec != null) {
            // Send initial response headers (HTTP/2 status 200 + gRPC content-type)
            Http2Headers responseHeaders = new DefaultHttp2Headers()
                                                   .status("200")
                                                   .add(HEADER_CONTENT_TYPE, GRPC_CONTENT_TYPE)
                                                   .add("grpc-encoding", "identity")
                                                   .add("grpc-accept-encoding", "identity,gzip");
            ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders));
        } else {
            ctx.fireChannelRead(frame);
        }
    }

    private void onDataReceived(ChannelHandlerContext ctx, Http2DataFrame frame) {
        try {
            if (codec == null) {
                sendTrailersOnly(
                        ctx, GrpcStatus.UNIMPLEMENTED.withMessage("Unhandled method %s", qualifiedMethodName)
                );
            }
            List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();

            ByteBuf buf = frame.content();
            MethodDescriptor md = codec.getMethodDescriptor(qualifiedMethodName);
            // --- gRPC wire format -------------------------------------------
            // Byte 0    : compressed-flag (0 = not compressed, 1 = compressed)
            // Bytes 1-4 : message length (big-endian unsigned 32-bit integer)
            // Bytes 5+  : serialized protobuf payload
            // ----------------------------------------------------------------

            if (buf.readableBytes() < GRPC_FRAME_HEADER_LENGTH) {
                sendTrailersOnly(ctx,
                        GrpcStatus.INTERNAL.withMessage("Incomplete gRPC frame header"));
                return;
            }

            byte compressionFlag = buf.readByte();
            int  messageLength   = buf.readInt();

            if (compressionFlag != GRPC_FRAME_NOT_COMPRESSED) {
                sendTrailersOnly(ctx,
                        GrpcStatus.UNIMPLEMENTED.withMessage("Compressed messages are not supported by this handler"));
                return;
            }

            if (buf.readableBytes() < messageLength) {
                sendTrailersOnly(ctx,
                        GrpcStatus.INTERNAL.withMessage("Declared message length %d exceeds available bytes %d", messageLength, buf.readableBytes()));
                return;
            }
            Object request = codec.decode(CodedInputStream.newInstance(buf.nioBuffer()), md.getInputType(), unknownFields);
            BiFunction<?, ChannelHandlerContext, ?> methodConsumer = transformers.get(qualifiedMethodName);
            byte[] response = codec.process(qualifiedMethodName, ctx, request, methodConsumer);
            writeGrpcDataFrame(ctx, response, false);
            sendTrailers(ctx, GrpcStatus.OK);
        } catch (IOException e) {
            sendTrailersOnly(ctx,
                    GrpcStatus.FAILED_PRECONDITION.withMessage(
                            "Invalid input", e));
        } finally {
            frame.release();
        }
    }

    void writeGrpcDataFrame(ChannelHandlerContext ctx, byte[] responseBytes, boolean endStream) {
        int totalLength = GRPC_FRAME_HEADER_LENGTH + responseBytes.length;
        ByteBuf buf = ctx.alloc().buffer(totalLength);

        buf.writeByte(GRPC_FRAME_NOT_COMPRESSED); // compression flag
        buf.writeInt(responseBytes.length);        // message length (4 bytes, big-endian)
        buf.writeBytes(responseBytes);             // serialized protobuf payload

        ctx.writeAndFlush(new DefaultHttp2DataFrame(buf, endStream));
    }

    private void sendTrailers(ChannelHandlerContext ctx, GrpcStatus status) {
        trailersSend(ctx, status, status.getHeaders());
    }

    private void sendTrailersOnly(ChannelHandlerContext ctx, GrpcStatus status) {
        Http2Headers trailersOnly = status.getHeaders()
                                          .status("200")
                                          .add(HEADER_CONTENT_TYPE, GRPC_CONTENT_TYPE);
        trailersSend(ctx, status, trailersOnly);
    }

    private void trailersSend(ChannelHandlerContext ctx, GrpcStatus status, Http2Headers headers) {
        if (status.isOk()) {
            logger.debug("Processing gRPC frame for method {}", qualifiedMethodName);
        } else {
            logger.warn("Processing gRPC frame for method {} with error status: {}", qualifiedMethodName, status);
        }
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));
    }

}
