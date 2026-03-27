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

@BuilderClass(CentreonAgent.Builder.class)
public class CentreonAgent extends ProtoBuf implements CodecProvider {

    @Setter
    public static class Builder extends ProtoBuf.Builder {
        @Override
        public CentreonAgent build() {
            return new CentreonAgent(this);
        }
    }
    public static CentreonAgent.Builder getBuilder() {
        return new CentreonAgent.Builder();
    }

    public CentreonAgent(CentreonAgent.Builder builder) {
        super(builder);
    }

    @Override
    protected BinaryCodec getDecoder(ProtoBuf.Builder builder)
            throws DescriptorValidationException, IOException {
        BinaryCodec decoder = new BinaryCodec("Centreon", CentreonAgent.class.getClassLoader().getResourceAsStream("centreon.binpb"));
        OpentelemetryDecoder otel = new OpentelemetryDecoder();
        decoder.addFastPath("opentelemetry.proto.common.v1.KeyValue", (BinaryCodec.MessageFastPathFunction<?>) otel::decodeKeyValue);
        decoder.addFastPath("opentelemetry.proto.common.v1.AnyValue", (BinaryCodec.MessageFastPathFunction<?>) otel::decodeAnyValue);
        decoder.addFastPath("opentelemetry.proto.resource.v1.Resource", (BinaryCodec.MessageFastPathFunction<?>) otel::decodeResource);
        decoder.addFastPath("opentelemetry.proto.metrics.v1.NumberDataPoint", (BinaryCodec.MessageFastPathFunction<?>) otel::decodeDataPoints);
        decoder.addFastPath("opentelemetry.proto.metrics.v1.ResourceMetrics.schema_url", (BinaryCodec.FieldFastPathFunction<?>) otel::decodeUrl);
        decoder.addFastPath("opentelemetry.proto.metrics.v1.ScopeMetrics.schema_url", (BinaryCodec.FieldFastPathFunction<?>) otel::decodeUrl);
        return decoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerFastPath(Factory factory, GrpcReceiver r) {
        factory.register("com.centreon.agent.AgentService.Export",
                (h, m) -> export(r, h, (Map<String, Object>) m)
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> export(GrpcReceiver r, GrpcStreamHandler handler, Map<String, Object> messageFrom) {
        if (messageFrom.containsKey("otel_request")) {
            r.publish(handler, (Map<String, Object>) messageFrom.get("otel_request"));
            return Map.of("otel_response", Map.of("partial_success", Map.of("rejected_data_points", 0L, "error_message", "")));
        } else if (messageFrom.containsKey("init")) {
            return Map.of("config", Map.of("centreon_version", Map.of("major", 24, "minor", 10, "patch", 0)));
        } else {
            return Map.of("error", "Invalid request format");
        }
    }

}
