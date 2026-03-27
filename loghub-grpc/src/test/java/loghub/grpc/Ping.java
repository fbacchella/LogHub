package loghub.grpc;

import java.io.IOException;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;

public class Ping extends BinaryCodec {

    public Ping() throws DescriptorValidationException, IOException {
        super("Ping", Ping.class.getResourceAsStream("/ping.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("ping.PingRequest", this::decodePingValue);
    }

    private String decodePingValue(CodedInputStream stream, Descriptor descriptor,
            List<UnknownField> unknownFields) throws IOException {
        int tag = stream.readTag();
        assert tag == 10 : "Expected tag for message field";
        return stream.readString();
    }

}
