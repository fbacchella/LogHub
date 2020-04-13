package loghub.decoders;

import loghub.FilterException;

public class DecodeException extends Exception {

    public static class RuntimeDecodeException extends RuntimeException {
        public RuntimeDecodeException(DecodeException cause) {
            super(cause.getMessage(), cause);
        }
        public RuntimeDecodeException(String message, DecodeException cause) {
            super(message, cause);
        }
        public DecodeException getDecodeException() {
            return (DecodeException) getCause();
        }
    }

    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
    public DecodeException(FilterException cause) {
        super("Filter exception", cause);
    }
    public DecodeException(String message) {
        super(message);
    }

}
