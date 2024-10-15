package loghub.protobuf;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import loghub.ProtobufTestUtils;
import lombok.Builder;
import lombok.Data;

public class TestBinaryDecoder {

    @Data
    @Builder
    private static class CustomSample {
        private double value;
        private Instant timeStamp;
    }

    @Test
    public void parsePrometheusWriteRequest() throws IOException, Descriptors.DescriptorValidationException {
        byte[] buffer = ProtobufTestUtils.getWriteRequest().toByteArray();
        BinaryDecoder decoder = ProtobufTestUtils.getPrometheusDecoder();
        decoder.addFastPath("prometheus.TimeSeries.samples", s -> {
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
        decoder.parseInput(CodedInputStream.newInstance(buffer), "prometheus.WriteRequest", values, unknownFields);
        Assert.assertEquals(1, values.size());
        Assert.assertEquals(0, unknownFields.size());
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> ts = (List<Map<String, Object>>) values.get("timeseries");
        Assert.assertEquals(1, ts.size());
        Map<String,Object> s1 = ts.get(0);
        @SuppressWarnings("unchecked")
        List<TestBinaryDecoder.CustomSample> samples = (List<CustomSample>) s1.get("samples");
        Assert.assertEquals(1, samples.size());
        samples.forEach(s -> Assert.assertTrue(s instanceof CustomSample));
        @SuppressWarnings("unchecked")
        List<TestBinaryDecoder.CustomSample> labels = (List<CustomSample>) s1.get("labels");
        Assert.assertEquals(2, labels.size());
    }

}
