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

import lombok.Data;

// Generate with the commands
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/*.proto --descriptor_set_out=loghub-protobuf/src/main/resources/prometheus.binpb -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
// $PROTOC_HOME/bin/protoc loghub-protobuf/src/main/protobuf/prometheus/* loghub-protobuf/src/main/protobuf/gogoproto/gogo.proto --java_out=loghub-protobuf/src/test/java/ -Iloghub-protobuf/src/main/protobuf -Iloghub-protobuf/src/main/protobuf -I$PROTOC_HOME/include
public class PrometheusDecoder extends BinaryDecoder {

    @Data
    private static class Sample {
        private final Instant timestamp;
        private final double value;
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

    private Object decodeTimeseries(CodedInputStream decodeSample, List<UnknownField> unknownFields) throws IOException {
        Map<String, Object> values = new HashMap<>();
        Map<String, String> labels = new HashMap<>();
        List<Sample> samples = new ArrayList<>();

        values.put("labels", labels);
        while (!decodeSample.isAtEnd()) {
            Descriptors.FieldDescriptor gd = resolve("prometheus.TimeSeries", decodeSample.readTag());
            switch (gd.getName()) {
            case "labels":
                Map.Entry<String, String> e = resolveFieldValue(decodeSample, gd, unknownFields);
                if ("__name__".equals(e.getKey())) {
                    values.put("name", e.getValue());
                } else {
                    labels.put(e.getKey(), e.getValue());
                }
                break;
            case "samples":
                Sample s = resolveFieldValue(decodeSample, gd, unknownFields);
                samples.add(s);
                break;
            default:
                values.put(gd.getName(), resolveFieldValue(decodeSample, gd, unknownFields));
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

    private Sample decodeSample(CodedInputStream decodeSample, List<UnknownField> unknownFields) throws IOException {
        double value = Double.NaN;
        long time =  0;
        while (!decodeSample.isAtEnd()) {
            Descriptors.GenericDescriptor gd = resolve("prometheus.Sample", decodeSample.readTag());
            switch (gd.getName()) {
            case "value":
                value = decodeSample.readDouble();
                break;
            case "timestamp":
                time = decodeSample.readInt64();
                break;
            }
        }
        Instant t = Instant.ofEpochMilli(time);
        return new Sample(t, value);
    }

    private Map.Entry<String, String> decodeLabel(CodedInputStream decodeLabel, List<UnknownField> unknownFields) throws IOException {
        String name = "";
        String value = "";
        while (!decodeLabel.isAtEnd()) {
            Descriptors.GenericDescriptor gd = resolve("prometheus.Label", decodeLabel.readTag());
            switch (gd.getName()) {
            case "name":
                name = decodeLabel.readString();
                break;
            case "value":
                value = decodeLabel.readString();
                break;
            }
        }
        return Map.entry(name, value);
    }

    public Map<String, Object> parseWriteRequest(ByteBuffer buffer) throws IOException {
        List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
        return parseInput(CodedInputStream.newInstance(buffer), "prometheus.WriteRequest", unknownFields);
    }

}
