package loghub.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

// Generate with the commands
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/*.proto --descriptor_set_out=loghub-protobuf/src/main/resources/prometheus.binpb -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/* loghub-protobuf/src/main/protobuf/gogoproto/gogo.proto --java_out=loghub-protobuf/src/test/java/ -Iloghub-protobuf/src/main/protobuf -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
public class PrometheusDecoder extends BinaryCodec {

    private record Sample(Instant timestamp, double value) {
    }

    public PrometheusDecoder() throws Descriptors.DescriptorValidationException, IOException {
        super(PrometheusDecoder.class.getClassLoader().getResourceAsStream("prometheus.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("prometheus.WriteRequest.timeseries", this::decodeTimeseries);
        addFastPath("prometheus.Sample", this::decodeSample);
        addFastPath("prometheus.Label", this::decodeLabel);
    }

    private Object decodeTimeseries(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        Map<String, Object> values = new HashMap<>();
        Map<String, String> labels = new HashMap<>();
        List<Sample> samples = new ArrayList<>();

        values.put("labels", labels);
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "labels":
                Map.Entry<String, String> e = resolveFieldValue(stream, gd, unknownFields);
                if ("__name__".equals(e.getKey())) {
                    values.put("name", e.getValue());
                } else {
                    labels.put(e.getKey(), e.getValue());
                }
                break;
            case "samples":
                Sample s = resolveFieldValue(stream, gd, unknownFields);
                samples.add(s);
                break;
            default:
                values.put(gd.getName(), resolveFieldValue(stream, gd, unknownFields));
                break;
            }
        }
        if (samples.size() == 1 && ! Double.isNaN(samples.get(0).value)) {
            Sample s = samples.get(0);
            values.put("sample", Map.of("timestamp", s.timestamp, "value", s.value));
        } else {
            List<?> samplesMap = samples.stream()
                                         .filter(s -> ! Double.isNaN(s.value))
                                         .map(s -> Map.of("timestamp", s.timestamp, "value", s.value))
                                         .collect(Collectors.toList());
            if (! samplesMap.isEmpty()) {
                values.put("sample", samplesMap);
            }
        }
        return values;
    }

    private Sample decodeSample(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        double value = Double.NaN;
        long time =  0;
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "value":
                value = stream.readDouble();
                break;
            case "timestamp":
                time = stream.readInt64();
                break;
            }
        }
        Instant t = Instant.ofEpochMilli(time);
        return new Sample(t, value);
    }

    private Map.Entry<String, String> decodeLabel(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields) throws IOException {
        String name = "";
        String value = "";
        while (!stream.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolveField(stream, descriptor);
            switch (gd.getName()) {
            case "name":
                name = stream.readString();
                break;
            case "value":
                value = stream.readString();
                break;
            }
        }
        return Map.entry(name, value);
    }

    public Map<String, Object> parseWriteRequest(ByteBuffer buffer) throws IOException {
        List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();
        return decode(CodedInputStream.newInstance(buffer), "prometheus.WriteRequest", unknownFields);
    }

}
