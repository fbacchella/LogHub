package loghub;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Descriptors;

import loghub.protobuf.BinaryDecoder;
import prometheus.Remote;
import prometheus.Types;

public class ProtobufTestUtils {
    public static Remote.WriteRequest getWriteRequest() {
        return Remote.WriteRequest.newBuilder().addTimeseries(
                Types.TimeSeries.newBuilder()
                        .addLabels(Types.Label.newBuilder().setName("__NAME__").setValue("test_event"))
                        .addLabels(Types.Label.newBuilder().setName("label").setValue("value")
                ).addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1))
        ).build();
    }

    public static BinaryDecoder getPrometheusDecoder() throws IOException, Descriptors.DescriptorValidationException {
        try (InputStream is = BinaryDecoder.class.getResourceAsStream("/prometheus.binpb")) {
            return new BinaryDecoder(is);
        }
    }

    private ProtobufTestUtils() {
        // Only static usage
    }
}
