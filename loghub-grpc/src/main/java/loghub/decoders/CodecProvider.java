package loghub.decoders;

import loghub.grpc.BinaryCodec;
import loghub.grpc.GrpcStreamHandler;
import loghub.receivers.GrpcReceiver;

public interface CodecProvider {
    BinaryCodec getProtobufCodec();
    void registerFastPath(GrpcStreamHandler.Factory factory, GrpcReceiver r);
}
