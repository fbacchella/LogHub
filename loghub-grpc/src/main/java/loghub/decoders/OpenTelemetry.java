package loghub.decoders;

import java.io.IOException;
import java.util.Map;

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

    private Map<String, Object> metricsExport(GrpcReceiver r, GrpcStreamHandler handler, Map<String, Object> metrics) {
        r.publish(handler, metrics);
        return Map.of("partial_success", Map.of("rejected_data_points", 0L, "error_message", ""));
    }

}
