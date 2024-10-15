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
        addFastPath("prometheus.TimeSeries.samples", this::decodeSample);
        addFastPath("prometheus.TimeSeries.labels", this::decodeLabel);
    }

    private Object decodeTimeseries(CodedInputStream decodeSample) throws IOException {
        Map<String, Object> values = new HashMap<>();
        Map<String, String> labels = new HashMap<>();
        List<Sample> samples = new ArrayList<>();

        values.put("labels", labels);
        List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
        while (!decodeSample.isAtEnd()) {
            Descriptors.GenericDescriptor gd = resolve("prometheus.TimeSeries", decodeSample.readTag());
            switch (gd.getName()) {
            case "labels":
                Map.Entry<String, String> e = parseMessage((Descriptors.FieldDescriptor)gd, decodeSample, unknownFields);
                if ("__name__".equals(e.getKey())) {
                    values.put("name", e.getValue());
                } else {
                    labels.put(e.getKey(), e.getValue());
                }
                break;
            case "samples":
                Sample s = parseMessage((Descriptors.FieldDescriptor)gd, decodeSample, unknownFields);
                samples.add(s);
                break;
            default:
                resolveValue((Descriptors.FieldDescriptor)gd, decodeSample, values, unknownFields);
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

    private Sample decodeSample(CodedInputStream decodeSample) throws IOException {
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

    private Map.Entry<String, String> decodeLabel(CodedInputStream decodeSample) throws IOException {
        String name = "";
        String value = "";
        while (!decodeSample.isAtEnd()) {
            Descriptors.GenericDescriptor gd = resolve("prometheus.Label", decodeSample.readTag());
            switch (gd.getName()) {
            case "name":
                name = decodeSample.readString();
                break;
            case "value":
                value = decodeSample.readString();
            }
        }
        return Map.entry(name, value);
    }

    public Map<String, Object> parseWriteRequest(ByteBuffer buffer) throws IOException {
        Map<String, Object> values = new HashMap<>();
        List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
        parseInput(CodedInputStream.newInstance(buffer), "prometheus.WriteRequest", values, unknownFields);
        return values;
    }

}
