package loghub.grpc;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;

public class Ping<C> extends BinaryCodec<C> {

    private final Descriptor pingResponse;

    public Ping() throws DescriptorValidationException, IOException {
        super(Ping.class.getResourceAsStream("/ping.binpb"));
        pingResponse = getMessageDescriptor("ping.PingResponse");
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("ping.PingService.Ping", this::ping);
        addFastPath("ping.PingRequest", this::decodePingValue);
    }

    private byte[] ping(String message, C context, BiFunction<String, C, Long> consumer) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(pingResponse);
        builder.setField(pingResponse.findFieldByName("message"), message);
        builder.setField(pingResponse.findFieldByName("timestamp"), consumer.apply(message, context));
        return builder.build().toByteArray();
    }

    private String decodePingValue(CodedInputStream stream, Descriptor descriptor,
            List<UnknownField> unknownFields) throws IOException {
        int tag = stream.readTag();
        assert tag == 10 : "Expected tag for message field";
        return stream.readString();
    }
}
