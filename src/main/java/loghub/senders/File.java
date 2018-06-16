package loghub.senders;

import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;

@AsyncSender
public class File extends Sender {

    private final CompletionHandler<Integer, Event> handler = new CompletionHandler<Integer, Event>() {

        @Override
        public void completed(Integer result, Event event) {
            event.getConnectionContext().acknowledge();
            CompletableFuture<Boolean> done = CompletableFuture.completedFuture(true);
            File.this.processStatus(event, done);
        }

        @Override
        public void failed(Throwable ex, Event event) {
            if (ex instanceof AsynchronousCloseException || ex instanceof ClosedChannelException) {
                CompletableFuture<Boolean> failed = new CompletableFuture<>();
                failed.complete(false);
                File.this.processStatus(event, failed);
            } else {
                CompletableFuture<Boolean> failed = new CompletableFuture<>();
                failed.completeExceptionally(ex);
                File.this.processStatus(event, failed);
            }
        }

    };

    private String fileName;
    private String separator = "";
    private byte[] separatorBytes = new byte[] {};
    private boolean truncate = false;

    private AsynchronousFileChannel destination;
    private AtomicLong position;

    @Override
    public boolean configure(Properties properties) {
        try {
            destination = AsynchronousFileChannel.open(Paths.get(fileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            // Failed to used StandardOpenOption.(APPEND vs CREATE), so truncate if need after creation
            if (truncate) {
                destination.truncate(0);
            }
            position = new AtomicLong(destination.size());
        } catch (IOException | UnsupportedOperationException e) {
            logger.error("error openening output file {}: {}", fileName, Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return false;
        }
        if (separator.length() > 0) {
            separatorBytes = separator.getBytes(StandardCharsets.UTF_8);
        }
        return super.configure(properties);
    }

    @Override
    public boolean send(Event event) {
        try {
            byte[] msg = getEncoder().encode(event);
            ByteBuf buffer = Unpooled.buffer(msg.length
                                             + separatorBytes.length);
            buffer.writeBytes(msg);
            buffer.writeBytes(separatorBytes);
            long writepose = position.getAndAdd(msg.length
                                                + separatorBytes.length);
            destination.write(buffer.nioBuffer(), writepose, event,
                              handler);
            return true;
        } catch (IllegalArgumentException | NonWritableChannelException e) {
            logger.error("error writing event to {}: {}", fileName,
                         Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return false;
        } 
    }

    /**
     * Used by unit test, don't use it
     * @throws IOException
     */
    void close() throws IOException {
        destination.close();
    }

    @Override
    public void stopSending() {
        if (destination.isOpen()) {
            try {
                // The lock will not be released, as you don't release a closed file
                destination.lock();
                destination.force(true);
                destination.close();
            } catch (IOException e) {
                logger.error("Failed to close {}: {}", fileName,
                             Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } 
        }
        super.stopSending();
    }

    @Override
    public String getSenderName() {
        return "File_" + fileName;
    }

    /**
     * @return the fileName
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * @return the separator
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * @param separator the separator to set
     */
    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

}
