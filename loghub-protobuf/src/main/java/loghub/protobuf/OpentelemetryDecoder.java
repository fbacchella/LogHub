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
public class OpentelemetryDecoder extends BinaryDecoder {

    public OpentelemetryDecoder() throws Descriptors.DescriptorValidationException, IOException {
        super(PrometheusDecoder.class.getClassLoader().getResourceAsStream("opentelemetry.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("opentelemetry.proto.common.v1.KeyValue", this::decodeKeyValue);
        addFastPath("opentelemetry.proto.common.v1.AnyValue", this::decodeAnyValue);
        addFastPath("opentelemetry.proto.metrics.v1.Sum.data_points", this::decodeDataPoints);
        addFastPath("opentelemetry.proto.metrics.v1.Gauge.data_points", this::decodeDataPoints);
    }

    private Object decodeDataPoints(CodedInputStream dataPointStream, List<UnknownField> unknownFields) throws IOException {
        Map<String, Object> content = new HashMap<>();
        while (!dataPointStream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolve("opentelemetry.proto.metrics.v1.NumberDataPoint",
                    dataPointStream.readTag());
            switch (gd.getName()) {
            case "start_time_unix_nano":
            case "time_unix_nano": {
                long timestampNanos = dataPointStream.readFixed64();
                long seconds = timestampNanos / 1_000_000_000; // Partie entière des secondes
                int nanos = (int) (timestampNanos % 1_000_000_000); // Reste en nanosecondes
                Instant instant = Instant.ofEpochSecond(seconds, nanos);
                content.put(gd.getName(), instant);
                break;
            }
            case "as_double": {
                content.put("value", dataPointStream.readDouble());
                break;
            }
            case "as_int": {
                content.put("value", dataPointStream.readSFixed64());
                break;
            }
            default:
                content.put(gd.getName(), resolveFieldValue(dataPointStream, gd, unknownFields));
            }
        }
        return content;
    }

    private Map.Entry<String, Object> decodeKeyValue(CodedInputStream keyValueStream, List<UnknownField> unknownFields) throws IOException {
        String name = "";
        Object value = null;
        while (!keyValueStream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolve("opentelemetry.proto.common.v1.KeyValue", keyValueStream.readTag());
            switch (gd.getName()) {
            case "key":
                name = keyValueStream.readString();
                break;
            case "value":
                value = readMessageField(keyValueStream, gd, unknownFields);
                break;
            default:
                throw new UnsupportedOperationException("Unreachable");
            }
        }
        return Map.entry(name, value);
    }

    private Object decodeAnyValue(CodedInputStream anyValueStream, List<UnknownField> unknownFields) throws IOException {
        if (!anyValueStream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolve("opentelemetry.proto.common.v1.AnyValue", anyValueStream.readTag());
            switch (gd.getName()) {
                case "string_value": return anyValueStream.readString();
                case "bool_value": return anyValueStream.readBool();
                case "int_value": return anyValueStream.readInt64();
            default:
                return readMessageField(anyValueStream, gd, unknownFields);
            }
        } else {
            return null;
        }
    }

    public Map<String, Object> parseWriteRequest(String messageType, ByteBuffer buffer) throws IOException {
        List<UnknownField> unknownFields = new ArrayList<>();
        return parseInput(CodedInputStream.newInstance(buffer), messageType, unknownFields);
    }

}
