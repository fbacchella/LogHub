package loghub.grpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

public class OpentelemetryDecoder<C> extends BinaryCodec<C> {

    public OpentelemetryDecoder() throws Descriptors.DescriptorValidationException, IOException {
        super(OpentelemetryDecoder.class.getClassLoader().getResourceAsStream("opentelemetry.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("opentelemetry.proto.common.v1.KeyValue", this::decodeKeyValue);
        addFastPath("opentelemetry.proto.common.v1.AnyValue", this::decodeAnyValue);
        addFastPath("opentelemetry.proto.resource.v1.Resource", this::decodeResource);
        addFastPath("opentelemetry.proto.metrics.v1.NumberDataPoint", this::decodeDataPoints);
        addFastPath("opentelemetry.proto.metrics.v1.ResourceMetrics.schema_url", this::decodeUrl);
        addFastPath("opentelemetry.proto.metrics.v1.ScopeMetrics.schema_url", this::decodeUrl);
        addFastPath("opentelemetry.proto.collector.metrics.v1.MetricsService.Export", this::metricsExport);
    }

    private Object decodeResource(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields)
            throws IOException {
        Map<String, Object> content = new HashMap<>();
        Map<String, Object> attributes = new HashMap<>();
        content.put("attributes", attributes);
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "attributes":
                Map.Entry<String, Object> kv = resolveFieldValue(stream, gd, unknownFields);
                attributes.put(kv.getKey(), kv.getValue());
                break;
            default:
                content.put(gd.getName(), resolveFieldValue(stream, gd, unknownFields));
            }
        }
        return content;
    }

    private Object decodeDataPoints(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        Map<String, Object> content = new HashMap<>();
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "start_time_unix_nano", "time_unix_nano" -> {
                long timestampNanos = stream.readFixed64();
                long seconds = timestampNanos / 1_000_000_000; // Partie entière des secondes
                int nanos = (int) (timestampNanos % 1_000_000_000); // Reste en nanosecondes
                Instant instant = Instant.ofEpochSecond(seconds, nanos);
                content.put(gd.getName(), instant);
            }
            case "as_double" ->
                content.put("value", stream.readDouble());
            case "as_int" ->
                content.put("value", stream.readSFixed64());
            default ->
                content.put(gd.getName(), resolveFieldValue(stream, gd, unknownFields));
            }
        }
        return content;
    }

    private Map.Entry<String, Object> decodeKeyValue(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        String name = "";
        Object value = null;
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "key":
                name = stream.readString();
                break;
            case "value":
                value = readMessageField(stream, gd, unknownFields);
                break;
            default:
                throw new UnsupportedOperationException("Unreachable");
            }
        }
        return Map.entry(name, value);
    }

    private Object decodeAnyValue(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        if (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            return switch (gd.getName()) {
                case "string_value" -> stream.readString();
                case "bool_value" -> stream.readBool();
                case "int_value" -> stream.readInt64();
                default -> readMessageField(stream, gd, unknownFields);
            };
        } else {
            return null;
        }
    }

    private Map<String, Object> metricsExport(Map<String, Object> metrics, C context, BiFunction<Map<String, Object>, C, Map<String, Object>> transformer) {
        return transformer.apply(metrics, context);
    }

    public Map<String, Object> parseWriteRequest(String messageType, ByteBuffer buffer) throws IOException {
        List<UnknownField> unknownFields = new ArrayList<>();
        return decode(CodedInputStream.newInstance(buffer), messageType, unknownFields);
    }

}
