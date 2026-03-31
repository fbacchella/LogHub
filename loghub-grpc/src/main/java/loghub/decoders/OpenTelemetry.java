package loghub.decoders;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import loghub.BuilderClass;
import loghub.grpc.BinaryCodec;
import loghub.grpc.GrpcStreamHandler;
import loghub.grpc.GrpcStreamHandler.Factory;
import loghub.grpc.OpentelemetryDecoder;
import loghub.receivers.GrpcReceiver;
import lombok.Setter;

@BuilderClass(OpenTelemetry.Builder.class)
public class OpenTelemetry extends ProtoBuf implements CodecProvider {

    @Setter
    public static class Builder extends ProtoBuf.Builder {
        @Override
        public OpenTelemetry build() {
            return new OpenTelemetry(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    public OpenTelemetry(Builder builder) {
        super(builder);
    }

    @Override
    protected BinaryCodec getDecoder(ProtoBuf.Builder builder) throws DescriptorValidationException, IOException {
        return new OpentelemetryDecoder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerFastPath(Factory factory, GrpcReceiver r) {
        factory.register("opentelemetry.proto.collector.metrics.v1.MetricsService.Export",
                (c, m) -> metricsExport(r, c,(Map<String, Object>) m)
        );
    }

    private Map<String, Object> metricsExport(GrpcReceiver r, GrpcStreamHandler<?, ?> handler, Map<String, Object> metricsData) {
        List<Map<String, Object>> resourceMetrics = (List<Map<String, Object>>) metricsData.get("resource_metrics");
        for(Map<String, Object> entry: resourceMetrics) {
            Map<String, Object> resource = (Map<String, Object>) entry.get("resource");
            resource.put("schema_url", entry.get("schema_url"));
            List<Map<String, Object>> scopeMetrics = (List<Map<String, Object>>) entry.get("scope_metrics");
            Stream<Map<String, Object>> metricsAsStream = scopeMetrics.stream().map(m -> {
                m.put("resource", resource);
                return m;
            }).flatMap(this::extractMetrics);
            r.publish(handler, metricsAsStream);
        }
        return Map.of("partial_success", Map.of("rejected_data_points", 0L, "error_message", ""));
    }

    private Stream<? extends Map<String, Object>> extractMetrics(Map<String, Object> scopeMetrics) {
        Map<String, Object> resource = (Map<String, Object>) scopeMetrics.get("resource");
        Map<String, Object> scope = (Map<String, Object>) scopeMetrics.get("scope");
        List<Map<String, Object>> metrics = (List<Map<String, Object>>) scopeMetrics.get("metrics");
        return metrics.stream().map(m -> {
            m.put("resource", resource);
            m.put("scope", scope);
            return m;
        });
    }

}
