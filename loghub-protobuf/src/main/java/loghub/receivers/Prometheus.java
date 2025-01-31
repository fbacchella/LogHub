package loghub.receivers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Map;

import com.google.protobuf.Descriptors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.events.Event;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;
import loghub.protobuf.PrometheusDecoder;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

@Blocking
@SelfDecoder
@BuilderClass(Prometheus.Builder.class)
public class Prometheus extends AbstractHttpReceiver<Prometheus, Prometheus.Builder> {

    @Setter
    public static class Builder extends AbstractHttpReceiver.Builder<Prometheus, Prometheus.Builder> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        protected boolean forwardMetas = false;
        @Override
        public Prometheus build() {
            return new Prometheus(this);
        }
    }
    public static Prometheus.Builder getBuilder() {
        return new Prometheus.Builder();
    }

    @ContentType("application/x-protobuf")
    @RequestAccept(methods = {"POST"})
    private class PrometheusWriteRequestHandler extends HttpRequestProcessing {
        private final ThreadLocal<Snappy> snappy = ThreadLocal.withInitial(Snappy::new);

        @Override
        protected void processRequest(FullHttpRequest request,
                ChannelHandlerContext ctx)
                throws HttpRequestFailure {
            logger.debug("Received request at {}", request::uri);
            ByteBuf content = request.content();
            ByteBuf uncompressed = content.duplicate();
            try {
                if ("snappy".equalsIgnoreCase(request.headers().get("Content-Encoding"))) {
                    uncompressed = ctx.alloc().buffer(content.readableBytes() * 20, content.readableBytes() * 20);
                    snappy.get().reset();
                    snappy.get().decode(content, uncompressed);
                } else {
                    uncompressed.retain();
                }
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                ConnectionContext<InetSocketAddress> cctx = Prometheus.this.getConnectionContext(ctx);
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                Prometheus.this.decode(cctx, request, uncompressed.nioBuffer());
            } catch (IllegalStateException | IOException ex) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? ex : null).log("Can't decode content: {}", () -> Helpers.resolveThrowableException(ex));
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, Helpers.resolveThrowableException(ex));
            } finally {
                uncompressed.release();
            }
            writeResponse(ctx, request, Unpooled.buffer(0), 0);
        }

    }

    private final Prometheus.PrometheusWriteRequestHandler bodyHandler;
    private final PrometheusDecoder decoder;

    public Prometheus(Builder builder) {
        super(builder);
        try {
            bodyHandler = new PrometheusWriteRequestHandler();
            decoder = new PrometheusDecoder();
        } catch (IOException | Descriptors.DescriptorValidationException ex) {
            throw new IllegalArgumentException(ex);
         }
    }

    @Override
    protected void modelSetup(ChannelPipeline pipeline) {
        // Prometheus don’t conform to HTTP compression standard for snappy content encoding
        pipeline.remove("HttpContentDeCompressor");
        pipeline.remove("HttpContentCompressor");
        pipeline.addLast(bodyHandler);
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "PrometheusReceiver";
    }

    @Override
    public String getReceiverName() {
        return "Prometheus/" + getListen() + "/" + getPort();
    }

    private void decode(ConnectionContext<InetSocketAddress> cctx, FullHttpRequest request, ByteBuffer buffer) throws IOException {
        Map<String, Object> values = decoder.parseWriteRequest(buffer);
        Event ev = mapToEvent(cctx, values);
        ev.putMeta("url_path", request.uri());
        HttpHeaders headers = request.headers();
        if (headers.contains("X-Prometheus-Remote-Write-Version")) {
            ev.putMeta("prometheus_remote_write_version", headers.get("X-Prometheus-Remote-Write-Version"));
        }
        if (headers.contains("User-Agent")) {
            ev.putMeta("user_agent", headers.get("User-Agent"));
        }
        ev.putMeta("host_header", headers.get("Host"));
        send(ev);
    }

}
