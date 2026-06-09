package loghub.decoders;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import loghub.BuilderClass;
import loghub.grpc.BinaryCodec;
import loghub.grpc.GrpcStreamHandler;
import loghub.grpc.GrpcStreamHandler.Factory;
import loghub.receivers.GrpcReceiver;
import lombok.Setter;

@BuilderClass(CentreonBroker.Builder.class)
public class CentreonBroker extends ProtoBuf implements CodecProvider {

    @Setter
    public static class Builder extends ProtoBuf.Builder {
        @Override
        public CentreonBroker build() {
            return new CentreonBroker(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public CentreonBroker(Builder builder) {
        super(builder);
    }

    @Override
    protected BinaryCodec getDecoder(ProtoBuf.Builder builder) throws DescriptorValidationException, IOException {
        return new BinaryCodec("Centreon", CentreonAgent.class.getClassLoader().getResourceAsStream("centreon.binpb"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerFastPath(Factory factory, GrpcReceiver r) {
        factory.register("com.centreon.broker.stream.centreon_bbdo.exchange",
                (c, m) -> exchange(r, c, (Map<String, Object>) m)
        );
    }

    private Map<String, Object> exchange(GrpcReceiver r, GrpcStreamHandler<?, ?> handler, Map<String, Object> message) {
        logger.trace("Received exchange message {}", message);
        r.publish(handler, Stream.of(message));

        return Map.of();
    }

}
