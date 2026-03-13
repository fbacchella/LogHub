package loghub.decoders;

import loghub.protobuf.BinaryCodec;
import loghub.protobuf.GrpcStreamHandler;
import loghub.receivers.GrpcReceiver;

public interface CodecProvider {
    BinaryCodec<GrpcStreamHandler> getProtobufCodec();
    void registerFastPath(GrpcStreamHandler.Factory factory, GrpcReceiver r);
}
