package loghub.grpc;

import java.util.Objects;

import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import loghub.Helpers;
import lombok.Getter;

@Getter
public class GrpcStatus {
    public static final GrpcStatus OK = new GrpcStatus(0, "OK");
    public static final GrpcStatus CANCELLED = new GrpcStatus(1, "Cancelled");
    public static final GrpcStatus UNKNOWN = new GrpcStatus(2, "Unknown");
    public static final GrpcStatus INVALID_ARGUMENT = new GrpcStatus(3, "Invalid argument");
    public static final GrpcStatus DEADLINE_EXCEEDED = new GrpcStatus(4, "Deadline exceeded");
    public static final GrpcStatus NOT_FOUND = new GrpcStatus(5, "Not found");
    public static final GrpcStatus ALREADY_EXISTS = new GrpcStatus(6, "Already exists");
    public static final GrpcStatus PERMISSION_DENIED = new GrpcStatus(7, "Permission denied");
    public static final GrpcStatus RESOURCE_EXHAUSTED = new GrpcStatus(8, "Resource exhausted");
    public static final GrpcStatus FAILED_PRECONDITION = new GrpcStatus(9, "Failed precondition");
    public static final GrpcStatus ABORTED = new GrpcStatus(10, "Aborted");
    public static final GrpcStatus OUT_OF_RANGE = new GrpcStatus(11, "Out of range");
    public static final GrpcStatus UNIMPLEMENTED = new GrpcStatus(12, "Unimplemented");
    public static final GrpcStatus INTERNAL = new GrpcStatus(13, "Internal");
    public static final GrpcStatus UNAVAILABLE = new GrpcStatus(14, "Unavailable");
    public static final GrpcStatus DATA_LOSS = new GrpcStatus(15, "Data loss");
    public static final GrpcStatus UNAUTHENTICATED = new GrpcStatus(16, "Unauthenticated");

    private final int status;
    private final String message;
    private GrpcStatus(int status, String message) {
        this.status = status;
        this.message = message;
    }

    public Http2Headers getHeaders() {
        DefaultHttp2Headers headers = (DefaultHttp2Headers) new DefaultHttp2Headers()
                                              .add("grpc-status",  Integer.toString(status));
        if (status != 0) {
            headers.add("grpc-message", message);
        }
        return headers;
    }

    public GrpcStatus withMessage(String message) {
        return new GrpcStatus(status, message);
    }
    public GrpcStatus withMessage(Throwable exception) {
        return new GrpcStatus(status, Helpers.resolveThrowableException(exception));
    }
    public GrpcStatus withMessage(String message, Throwable exception) {
        return new GrpcStatus(status, message + ": " + Helpers.resolveThrowableException(exception));
    }
    public GrpcStatus withMessage(String message, Object... args) {
        return new GrpcStatus(status, String.format(message, args));
    }
    public boolean isOk() {
        return status == 0;
    }

    @Override
    public String toString() {
        return message + " (" + status + ")";
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof GrpcStatus that))
            return false;

        return status == that.status && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        int result = status;
        result = 31 * result + Objects.hashCode(message);
        return result;
    }

}
