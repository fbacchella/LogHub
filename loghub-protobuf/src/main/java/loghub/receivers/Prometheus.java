package loghub.receivers;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;
import loghub.protobuf.BinaryDecoder;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

// Generate with the commands
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/*.proto --descriptor_set_out=loghub-protobuf/src/main/resources/prometheus.binpb -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/* loghub-protobuf/src/main/protobuf/gogoproto/gogo.proto --java_out=loghub-protobuf/src/test/java/ -Iloghub-protobuf/src/main/protobuf -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
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
        private final Snappy snappy = new Snappy();
        private final BinaryDecoder decoder;

        PrometheusWriteRequestHandler() throws IOException, Descriptors.DescriptorValidationException {
            try (InputStream is = BinaryDecoder.class.getClassLoader().getResourceAsStream("prometheus.binpb")) {
                decoder = new BinaryDecoder(is);
            }
        }

        @Override
        protected void processRequest(FullHttpRequest request,
                ChannelHandlerContext ctx)
                throws HttpRequestFailure {
            logger.debug("Received request at {}", request::uri);
            ByteBuf content = request.content();
            ByteBuf uncompressed = ctx.alloc().buffer(content.readableBytes() * 2, content.readableBytes() * 20);
            byte[] inBufferArray;
            try {
                snappy.reset();
                snappy.decode(content, uncompressed);
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                ByteBuffer inBuffer = uncompressed.nioBuffer();
                inBufferArray = new byte[inBuffer.remaining()];
                inBuffer.get(inBufferArray);
                if (p != null) {
                    ConnectionContext<InetSocketAddress> cctx = Prometheus.this.getConnectionContext(ctx);
                    cctx.setPrincipal(p);
                }
                Map<String, Object> values = new HashMap<>();
                List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
                decoder.parseInput(CodedInputStream.newInstance(uncompressed.nioBuffer()), "prometheus.WriteRequest", values, unknownFields);
                if (values.containsKey("TimeSeries")) {
                    ((List<Map<String, Object>>) values.get("TimeSeries")).forEach(Prometheus.this::processTimeSerie);
                } else if (values.containsKey("MetricMetadata") && Prometheus.this.forwardMetas) {
                    ((List<Map<String, Object>>) values.get("MetricMetadata")).forEach(Prometheus.this::processMetadata);
                } else {
                    System.err.println(values.keySet());
                }
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
    private final boolean forwardMetas;

    public Prometheus(Builder builder) {
        super(builder);
        this.forwardMetas = builder.forwardMetas;
        try {
            bodyHandler = new PrometheusWriteRequestHandler();
        } catch (IOException | Descriptors.DescriptorValidationException ex) {
            throw new IllegalArgumentException(ex);
         }
    }

    @Override
    protected void modelSetup(ChannelPipeline pipeline) {
        // Prometheus don’t conform to HTTP compression standard
        pipeline.remove("HttpContentDeCompressor");
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

    private void processTimeSerie(Map<String, Object> timeSerie) {
        Event ev = Prometheus.this.getEventsFactory().newEvent();
        @SuppressWarnings("unchecked")
        List<Map<String, String>> labels = (List<Map<String, String>>) timeSerie.get("Label");
        String promName = null;
        for (Map<String, String> labelData: labels) {
            String label = labelData.get("name");
            if ("__name__".equals(label)) {
                promName = labelData.get("value");
            } else {
                ev.putAtPath(VariablePath.of("labels", labelData.get("name")), labelData.get("value"));
            }
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> samples = (List<Map<String, Object>>) timeSerie.get("Sample");
        if (samples.size() == 1 && promName != null) {
            ev.setTimestamp(samples.get(0).get("timestamp"));
            VariablePath vp = VariablePath.of(promName);
            Optional.ofNullable(samples.get(0).get("value")).map(Double.class::cast).ifPresent(v -> ev.putAtPath(vp, v));
        } else {
            logger.warn("Unhandled event data{}", () -> (logger.isDebugEnabled() ? "\n" + timeSerie : ""));
        }
        send(ev);
    }

    private void processMetadata(Map<String, Object> metadata) {
        Event ev = Prometheus.this.getEventsFactory().newEvent();
        ev.put("MetricMetadata", metadata);
        send(ev);
    }

}
