package loghub.grpc;

import java.util.Map;
import java.util.stream.Stream;

import loghub.receivers.GrpcReceiver;

public abstract class GrpcReceiverProcessor<O> extends GrpcProcessor<GrpcReceiver, Map<String, Object>, O> {

    public abstract static class Builder<B extends GrpcReceiverProcessor<O>, O> extends GrpcProcessor.Builder<B, GrpcReceiver, Map<String, Object>, O> {
    }

    protected GrpcReceiverProcessor(Builder<? extends GrpcReceiverProcessor<O>, O> builder) {
        super(builder);
    }

    protected void publish(GrpcStreamHandler<?, ?> handler, Stream<Map<String, Object>> content) {
        server.publish(handler, content);
    }


}
