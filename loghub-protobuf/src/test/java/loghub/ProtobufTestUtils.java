package loghub;

import prometheus.Remote;
import prometheus.Types;

public class ProtobufTestUtils {

    public static Remote.WriteRequest getWriteRequest() {
        return Remote.WriteRequest.newBuilder().addTimeseries(
                Types.TimeSeries.newBuilder()
                        .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_event"))
                        .addLabels(Types.Label.newBuilder().setName("label").setValue("value")
                ).addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1))
        ).build();
    }

    private ProtobufTestUtils() {
        // Only static usage
    }
}
