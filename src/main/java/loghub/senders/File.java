package loghub.senders;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.BuilderClass;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import lombok.Getter;
import lombok.Setter;

@AsyncSender
@BuilderClass(File.Builder.class)
public class File extends Sender {

    public static class Builder extends Sender.Builder<File> {
        @Setter
        private String fileName;
        @Setter
        private String separator = "";
        @Setter
        private boolean truncate = false;
        @Override
        public File build() {
            return new File(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }
  
    private final CompletionHandler<Integer, Event> handler = new CompletionHandler<Integer, Event>() {

        @Override
        public void completed(Integer result, Event event) {
            File.this.processStatus(event, true);
        }

        @Override
        public void failed(Throwable ex, Event event) {
            File.this.handleException(ex);
            File.this.processStatus(event, false);
        }

    };

    @Getter
    private final String fileName;
    private final byte[] separatorBytes;
    private final boolean truncate;

    private AsynchronousFileChannel destination;
    private AtomicLong position;

    public File(Builder builder) {
        super(builder);
        if (builder.separator.length() > 0) {
            separatorBytes = builder.separator.getBytes(StandardCharsets.UTF_8);
        } else {
            separatorBytes = new byte[] {};;
        }
        fileName = builder.fileName;
        truncate = builder.truncate;
    }


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
        return super.configure(properties);
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
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
     */
    @Override
    public void close() {
        try {
            destination.close();
        } catch (IOException e) {
            logger.error("Failed to close {}: {}", fileName,
                         Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        }
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

}
