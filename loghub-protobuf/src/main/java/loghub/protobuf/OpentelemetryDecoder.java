package loghub.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

// Generate with the commands
// $PROTOC_HOME/bin/protoc $(find loghub-protobuf/src/main/protobuf/opentelemetry -name '*.proto') --descriptor_set_out=loghub-protobuf/src/main/resources/opentelemetry.binpb -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
public class OpentelemetryDecoder extends BinaryCodec {

    public OpentelemetryDecoder() throws Descriptors.DescriptorValidationException, IOException {
        super(PrometheusDecoder.class.getClassLoader().getResourceAsStream("opentelemetry.binpb"));
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
            case "start_time_unix_nano":
            case "time_unix_nano": {
                long timestampNanos = stream.readFixed64();
                long seconds = timestampNanos / 1_000_000_000; // Partie entière des secondes
                int nanos = (int) (timestampNanos % 1_000_000_000); // Reste en nanosecondes
                Instant instant = Instant.ofEpochSecond(seconds, nanos);
                content.put(gd.getName(), instant);
                break;
            }
            case "as_double": {
                content.put("value", stream.readDouble());
                break;
            }
            case "as_int": {
                content.put("value", stream.readSFixed64());
                break;
            }
            default:
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
            switch (gd.getName()) {
                case "string_value": return stream.readString();
                case "bool_value": return stream.readBool();
                case "int_value": return stream.readInt64();
            default:
                return readMessageField(stream, gd, unknownFields);
            }
        } else {
            return null;
        }
    }

    public Map<String, Object> parseWriteRequest(String messageType, ByteBuffer buffer) throws IOException {
        List<UnknownField> unknownFields = new ArrayList<>();
        return decode(CodedInputStream.newInstance(buffer), messageType, unknownFields);
    }

}
