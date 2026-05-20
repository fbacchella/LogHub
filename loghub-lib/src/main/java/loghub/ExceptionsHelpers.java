package loghub;

import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.AccessDeniedException;

import javax.net.ssl.SSLHandshakeException;

public class ExceptionsHelpers {

    /**
     * Check if a Throwable is fatal hence should never be caught.
     * Thanks to superbaloo for the tips
     * @param err the exception to check.
     * @return true if the exception is fatal to the JVM and should not be caught in a plugin
     */
    public static boolean isFatal(Throwable err) {
        return (
                // StackOverflowError is a VirtualMachineError but not critical if found in a plugin
                ! (err instanceof StackOverflowError) &&
                        // VirtualMachineError includes OutOfMemoryError and other fatal errors
                        (err instanceof VirtualMachineError || err instanceof InterruptedException || err instanceof ThreadDeath));
    }

    /**
     * It tries to extract a meaningful message for any exception
     * @param t The exception to test
     * @return The extracted message
     */
    public static String resolveThrowableException(Throwable t) {
        StringBuilder builder = new StringBuilder();
        String lastMessage = "";
        while (t.getCause() != null) {
            String message = t.getMessage();
            if (message == null) {
                message = t.getClass().getSimpleName();
            }
            if (! lastMessage.endsWith(message)) {
                builder.append(message).append(": ");
            }
            lastMessage = message;
            t = t.getCause();
        }
        String message = t.getMessage();
        // Helping resolve bad exception's message
        if (t instanceof NoSuchMethodException) {
            message = "No such method: " + t.getMessage();
        } else if (t instanceof NegativeArraySizeException) {
            message = "Negative array size: " + message;
        } else if (t instanceof ArrayIndexOutOfBoundsException) {
            message = "Array out of bounds: " + message;
        } else if (t instanceof ClassNotFoundException) {
            message = "Class not found: " + message;
        } else if (t instanceof IllegalCharsetNameException) {
            message = "Illegal charset name: " + t.getMessage();
        } else if (t instanceof UnsupportedCharsetException) {
            message = "Unsupported charset name: " + t.getMessage();
        } else if (t instanceof AccessDeniedException) {
            message = "Access denied to file " + t.getMessage();
        } else if (t instanceof ClosedChannelException) {
            message = "Closed channel";
        } else if (t instanceof UnknownHostException) {
            message = "Unknown host \"%s\"".formatted(t.getMessage());
        } else if (t instanceof SSLHandshakeException) {
            // SSLHandshakeException is a chain of the same message, keep the last one
            builder.setLength(0);
        } else if (t instanceof InterruptedException) {
            builder.setLength(0);
            message = "Interrupted";
        } else if (message == null) {
            message = t.getClass().getSimpleName();
        } else if (lastMessage.endsWith(message)) {
            message = "";
            // Remove the last ": "
            builder.delete(builder.length() - 2, builder.length());
        }
        builder.append(message);
        return builder.toString();
    }

}
