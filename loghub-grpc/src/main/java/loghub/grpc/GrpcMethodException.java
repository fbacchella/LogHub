package loghub.grpc;

public class GrpcMethodException extends Exception {
    private final GrpcStatus status;

    public GrpcMethodException(GrpcStatus status) {
        super(status.getMessage());
        this.status = status;
    }

    public GrpcStatus getStatus() {
        return status;
    }
}
