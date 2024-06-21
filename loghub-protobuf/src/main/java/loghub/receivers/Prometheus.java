package loghub.receivers;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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

// Generate
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
            ByteBuf uncompressed = ctx.alloc().buffer(content.readableBytes() * 2, content.readableBytes() * 8);
            byte[] inBufferArray = new byte[]{};
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
                ((List<Map<String, Object>>) values.get("TimeSeries")).forEach(m -> {
                    Event ev = Prometheus.this.getEventsFactory().newEvent();
                    // [Fri Jun 21 16:24:20 CEST 2024]{Label=[{name=__name__, value=prometheus_engine_query_duration_seconds}, {name=instance, value=localhost:9090}, {name=job, value=prometheus}, {name=quantile, value=0.5}, {name=slice, value=result_sort}], Sample=[{value=NaN, timestamp=1718979859422}]}#{}
                    List<Map<String, String>> labels = (List<Map<String, String>>) m.get("Label");
                    for (Map<String, String> labelData: labels) {
                        String label = labelData.get("name");
                        if ("__name__".equals(label)) {
                            ev.put("name", labelData.get("value"));
                        } else {
                            ev.putAtPath(VariablePath.of("labels", labelData.get("name")), labelData.get("value"));
                        }
                     }
                    List<Map<String, Object>> samples = (List<Map<String, Object>>) m.get("Sample");
                    if (samples.size() == 1) {
                        ev.setTimestamp(samples.get(0).get("timestamp"));
                        Optional.ofNullable(samples.get(0).get("value")).map(Double.class::cast).ifPresent(v -> ev.put("value", v));
                    }
                    //System.err.println(ev);
                    //ev.drop();
                    send(ev);
                });
            } catch (IllegalStateException ex) {
                System.err.println(request.uri());
                request.headers().entries().forEach(e -> {
                    System.err.format("  %s: %s%n", e.getKey(), e.getValue());
                });
                /*System.err.println("  " + request.uri());
                if (inBufferArray.length > 0) {
                    logger.atError().withThrowable(ex).log(ex.getMessage());
                    UUID uuid = UUID.randomUUID();
                    Path p = Path.of("", "data", "loghub", "logs", UUID.randomUUID() + ".dat");
                    try {
                        Files.write(p, inBufferArray);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }*/
            } catch (IOException ex) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? ex : null).log("Can't decode content: {}", () -> Helpers.resolveThrowableException(ex));
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, Helpers.resolveThrowableException(ex));
            } finally {
                uncompressed.release();
            }
            writeResponse(ctx, request, Unpooled.buffer(0), 0);
        }

    }

    private final Prometheus.PrometheusWriteRequestHandler bodyHandler;

    public Prometheus(Builder builder) {
        super(builder);
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

}
