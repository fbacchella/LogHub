package loghub.grpc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.netty.channel.ChannelHandlerContext;
import loghub.BuilderClass;
import lombok.Setter;

@BuilderClass(OpenTelemetry.Builder.class)
public class OpenTelemetry extends GrpcReceiverProcessor<Map<String, Object>> {

    @Setter
    public static class Builder extends GrpcReceiverProcessor.Builder<OpenTelemetry, Map<String, Object>> {
        @Override
        public OpenTelemetry build() {
            return new OpenTelemetry(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final OpentelemetryDecoder decoder;

    public OpenTelemetry(Builder builder) {
        super(builder);
        try {
            decoder = new OpentelemetryDecoder();
        } catch (DescriptorValidationException | IOException | RuntimeException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public BinaryCodec getProtobufCodec() {
        return decoder;
    }

    @Override
    public GrpcService<Map<String, Object>, Map<String, Object>> getHandler(
            GrpcStreamHandler<Map<String, Object>, Map<String, Object>> handler, String qualifiedMethodName,
            ChannelHandlerContext ctx
    ) {
        return switch (qualifiedMethodName) {
            case "opentelemetry.proto.collector.metrics.v1.MetricsService.Export" ->
                    GrpcService.of(decoder, qualifiedMethodName, i -> metricsExport(handler, i));
            default -> null;
        };
    }

    private Map<String, Object> metricsExport(GrpcStreamHandler<?, ?> handler, Map<String, Object> metricsData) {
        List<Map<String, Object>> resourceMetrics = (List<Map<String, Object>>) metricsData.get("resource_metrics");
        for (Map<String, Object> entry : resourceMetrics) {
            Map<String, Object> resource = (Map<String, Object>) entry.get("resource");
            resource.put("schema_url", entry.get("schema_url"));
            List<Map<String, Object>> scopeMetrics = (List<Map<String, Object>>) entry.get("scope_metrics");
            Stream<Map<String, Object>> metricsAsStream = scopeMetrics.stream().map(m -> {
                m.put("resource", resource);
                return m;
            }).flatMap(this::extractMetrics);
            publish(handler, metricsAsStream);
        }
        return Map.of("partial_success", Map.of("rejected_data_points", 0L, "error_message", ""));
    }

    private Stream<Map<String, Object>> extractMetrics(Map<String, Object> scopeMetrics) {
        Map<String, Object> resource = (Map<String, Object>) scopeMetrics.get("resource");
        Map<String, Object> scope = (Map<String, Object>) scopeMetrics.get("scope");
        List<Map<String, Object>> metrics = (List<Map<String, Object>>) scopeMetrics.get("metrics");
        return metrics.stream().map(m -> {
            m.put("resource", Map.copyOf(resource));
            m.put("scope", Map.copyOf(scope));
            return m;
        });
    }

    @Override
    public String getServiceName() {
        return "opentelemetry.proto.collector.metrics.v1.MetricsService";
    }
}
