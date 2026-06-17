package loghub.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.ReadOnlyHttp2Headers;
import io.netty.util.AsciiString;
import loghub.Helpers;
import loghub.netty.HttpChannelConsumer;
import lombok.Getter;

public class GrpcStreamHandler<I, O> extends SimpleChannelInboundHandler<Http2DataFrame> {

    private static final Logger logger = LogManager.getLogger();

    // gRPC length-prefixed message framing constants
    private static final int GRPC_FRAME_HEADER_LENGTH = 5; // 1 byte flag + 4 bytes length
    private static final byte GRPC_FRAME_NOT_COMPRESSED = 0x00;

    public static <S, I, O> GrpcStreamHandler<I, O> of(GrpcProcessor<S, I, O> processor, Http2Headers requestHeaders, Duration grpcTimout, String qualifiedMethodName, ChannelHandlerContext ctx, GrpcStats stats, String encoding) {
        return new GrpcStreamHandler<>(s -> processor.getHandler(s, qualifiedMethodName, ctx), ctx, stats, encoding, requestHeaders, grpcTimout);
    }


    private final GrpcStats stats;
    private final ByteBuf currentMessage;

    @Getter
    private ChannelHandlerContext currentContext;
    @Getter
    private final Http2Headers requestHeaders;
    private final GrpcService<I, O> service;
    private final String encoding;

    private GrpcStreamHandler(Function<GrpcStreamHandler<I, O>, GrpcService<I, O>> getter, ChannelHandlerContext ctx, GrpcStats stats, String encoding,
            Http2Headers requestHeaders, Duration grpcTimout) {
        this.stats = stats;
        this.service = getter.apply(this);
        this.currentMessage = ctx.alloc().buffer();
        this.encoding = encoding;
        this.requestHeaders = readOnlyHeaders(requestHeaders);
    }

    private Http2Headers readOnlyHeaders(Http2Headers sourceHeaders) {
        AsciiString methodName = AsciiString.of(sourceHeaders.method());
        AsciiString path = AsciiString.of(sourceHeaders.path());
        AsciiString scheme = AsciiString.of(sourceHeaders.scheme());
        AsciiString authority = AsciiString.of(sourceHeaders.authority());

        List<AsciiString> others = new ArrayList<>(sourceHeaders.size());
        for (Map.Entry<CharSequence, CharSequence> entry : sourceHeaders) {
            CharSequence name = entry.getKey();
            if (!name.isEmpty() && name.charAt(0) != ':') {
                others.add(AsciiString.of(name));
                others.add(AsciiString.of(entry.getValue()));
            }
        }

        return ReadOnlyHttp2Headers.clientHeaders(false, methodName, path, scheme, authority,
                others.toArray(new AsciiString[0]));
    }

    protected void channelRead0(ChannelHandlerContext ctx, Http2DataFrame frame) {
        logger.debug("Data frame {}", frame);
        try {
            List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();

            ByteBuf buf = frame.content();
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
                stats.received(currentMessage);
                ByteBuf messageBuf = decompress(compressed, currentMessage.readSlice(messageLength));
                I request = service.decodeRequest(messageBuf, unknownFields);
                currentContext = ctx;
                O processed = service.doProcessing(request);
                byte[] response = switch (processed) {
                    case byte[] r ->  r;
                    case Map<?, ?> m -> service.encodeResponse((Map<String, Object>) m);
                    case null -> null;
                    default -> service.encodeResponse(processed);
                };
                if (response != null) {
                    writeGrpcDataFrame(ctx, response);
                }
            }

            if (frame.isEndStream()) {
                if (currentMessage.readableBytes() > 0) {
                    logger.trace("Discarded bytes from HTTP/2 frame: {}", currentMessage.readableBytes());
                }
                sendTrailers(ctx.channel(), GrpcStatus.OK);
            }
        } catch (IOException ex) {
            sendTrailers(ctx.channel(), GrpcStatus.FAILED_PRECONDITION.withMessage("Invalid input", ex));
        } catch (RuntimeException ex) {
            logger.atError().withThrowable(ex).log("{}: {}", requestHeaders::path, () -> Helpers.resolveThrowableException(ex));
            sendTrailers(ctx.channel(), GrpcStatus.INTERNAL.withMessage("Internal error processing gRPC request"));
        } catch (GrpcMethodException e) {
            sendTrailers(ctx.channel(), e.getStatus());
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

    private void writeGrpcDataFrame(ChannelHandlerContext ctx, byte[] responseBytes) {
        writeGrpcDataFrame(ctx.channel(), responseBytes);
    }

    public void writeGrpcDataFrame(Channel channel, byte[] responseBytes) {
        int totalLength = GRPC_FRAME_HEADER_LENGTH + responseBytes.length;
        ByteBuf buf = channel.alloc().buffer(totalLength);

        buf.writeByte(GRPC_FRAME_NOT_COMPRESSED); // compression flag
        buf.writeInt(responseBytes.length);        // message length (4 bytes, big-endian)
        buf.writeBytes(responseBytes);             // serialized protobuf payload

        channel.writeAndFlush(new DefaultHttp2DataFrame(buf, false));
    }

    public void sendTrailers(Channel channel, GrpcStatus status) {
        currentMessage.release();
        if (status.isOk()) {
            logger.debug("{} done", requestHeaders::path);
        } else {
            logger.warn("{} failed with error status: {}", requestHeaders::path, () -> status);
        }
        channel.writeAndFlush(new DefaultHttp2HeadersFrame(status.getHeaders(), true)).addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("Failed to send trailers: {}", f.cause().getMessage());
            }
            Long startTime = channel.attr(HttpChannelConsumer.STARTTIMEATTRIBUTE).get();
            stats.stats(startTime, 200, status);
        });
    }



    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt == GrpcService.CLOSE_EVENT) {
            service.onClose();
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    public String getQualifiedMethodName() {
        return service.getQualifiedMethodName();
    }
}
