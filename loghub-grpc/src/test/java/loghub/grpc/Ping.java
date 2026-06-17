package loghub.grpc;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;

public class Ping extends BinaryCodec {

    public record PingRequest(int tag, String message) {
    }

    public record PingResponse(String message, long timestamp) {
    }

    public Ping() throws DescriptorValidationException, IOException {
        super("Ping", Ping.class.getResourceAsStream("/ping.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("ping.PingRequest", this::decodePingValue);
    }

    private PingRequest decodePingValue(CodedInputStream stream, Descriptor descriptor,
            List<UnknownField> unknownFields) throws IOException {
        return new PingRequest(stream.readTag(), stream.readString());
    }

    @Override
    public Optional<byte[]> encode(Object o) {
        return switch (o) {
            case PingResponse req -> {
                Descriptors.Descriptor descriptor = getMessageDescriptor("ping.PingResponse");
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
                builder.setField(descriptor.findFieldByName("timestamp"), req.timestamp);
                builder.setField(descriptor.findFieldByName("message"), req.message);
                yield Optional.of(builder.build().toByteArray());
            }
            default -> Optional.empty();
        };
    }

}
