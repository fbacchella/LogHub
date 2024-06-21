package loghub.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import lombok.Builder;
import lombok.Data;
import prometheus.Remote;
import prometheus.Types;

public class TestBinaryDecoder {

    private Remote.WriteRequest getWriteRequest() {
        /*
         * See https://prometheus.io/docs/specs/remote_write_spec/ for documentation
         * generate code with protoc src/main/protobuf/prometheus.proto --java_out=src/test/java/
         */
        return Remote.WriteRequest.newBuilder().addTimeseries(
                Types.TimeSeries.newBuilder().addLabels(
                        Types.Label.newBuilder().setName("label").setValue("value")
                ).addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1)).addSamples(
                        Types.Sample.newBuilder().setValue(-1.0).setTimestamp(2))
        ).build();
    }

    @Data
    @Builder
    private static class CustomSample {
        private double value;
        private Instant timeStamp;
    }

    /*
     * The binpb file was generated with $PROTOC_HOME/bin/protoc src/main/resouces/protobuf/prometheus.proto --descriptor_set_out=src/test/resources/prometheus.binpb -Isrc/test/protobuf
     */
    @Test
    public void parsePrometheusWriteRequest() throws IOException, Descriptors.DescriptorValidationException {
        byte[] buffer = getWriteRequest().toByteArray();
        BinaryDecoder decoder;
        try (InputStream is = BinaryDecoder.class.getClassLoader().getResourceAsStream("prometheus.binpb")) {
            decoder = new BinaryDecoder(is);
        }
        decoder.addFastPath("loghub.protobuf.prometheus.TimeSeries.samples", s -> {
            CustomSample.CustomSampleBuilder csb = CustomSample.builder();
            while (! s.isAtEnd()) {
                int tag = s.readTag();
                int fieldNumber = (tag >> 3);
                if (fieldNumber == 1) {
                    csb.value(s.readDouble());
                } else if (fieldNumber == 2) {
                    csb.timeStamp(Instant.ofEpochMilli(s.readInt64()));
                }
            }
             return csb.build();
        });
        Map<String, Object> values = new HashMap<>();
        List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
        decoder.parseInput(CodedInputStream.newInstance(buffer), "loghub.protobuf.prometheus.WriteRequest", values, unknownFields);
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> ts = (List<Map<String, Object>>) values.get("TimeSeries");
        Assert.assertEquals(1, ts.size());
        Map<String,Object> s1 = ts.get(0);
        @SuppressWarnings("unchecked")
        List<TestBinaryDecoder.CustomSample> samples = (List<CustomSample>) s1.get("Sample");
        Assert.assertEquals(2, samples.size());
        samples.forEach(s -> Assert.assertTrue(s instanceof CustomSample));
    }

}
