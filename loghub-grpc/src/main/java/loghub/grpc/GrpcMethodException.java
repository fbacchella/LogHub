package loghub.grpc;

import lombok.Getter;

@Getter
public class GrpcMethodException extends Exception {
    private final GrpcStatus status;

    public GrpcMethodException(GrpcStatus status) {
        super(status.getMessage());
        this.status = status;
    }

}
