package loghub.encoders;

import loghub.FilterException;

public class EncodeException extends Exception {

    public static class RuntimeDecodeException extends RuntimeException {
        public RuntimeDecodeException(EncodeException cause) {
            super(cause.getMessage(), cause);
        }
        public RuntimeDecodeException(String message, EncodeException cause) {
            super(message, cause);
        }
        public EncodeException getDecodeException() {
            return (EncodeException) getCause();
        }
    }

    public EncodeException(FilterException cause) {
        super("Filter exception", cause);
    }
    public EncodeException(String message, Throwable cause) {
        super(message, cause);
    }
    public EncodeException(String message) {
        super(message);
    }
}
