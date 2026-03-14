package loghub.grpc;

@FunctionalInterface
public interface MethodProcessor<I, O> {
    O doProcessing(GrpcStreamHandler handler, I input) throws GrpcMethodException;
}
