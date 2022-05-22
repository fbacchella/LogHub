package loghub.senders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import lombok.Getter;
import lombok.Setter;

@AsyncSender
@CanBatch
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
  
    private final CompletionHandler<Integer, Event> handler = new CompletionHandler<>() {

        @Override
        public void completed(Integer result, Event event) {
            File.this.processStatus(event, true);
        }

        @Override
        public void failed(Throwable ex, Event event) {
            File.this.handleException(ex, event);
        }

    };

    @Getter
    private final Path fileName;
    private final byte[] separatorBytes;
    private final boolean truncate;

    private AsynchronousFileChannel destination;
    private AtomicLong position;

    public File(Builder builder) {
        super(builder);
        if (! builder.separator.isEmpty()) {
            separatorBytes = builder.separator.getBytes(StandardCharsets.UTF_8);
        } else {
            separatorBytes = new byte[] {};
        }
        fileName = Paths.get(builder.fileName);
        truncate = builder.truncate;
    }

    @Override
    public boolean configure(Properties properties) {
        ExecutorService executor = Executors.newCachedThreadPool(ThreadBuilder.get()
                                                                              .setDaemon(true)
                                                                              .getFactory("AsyncIO"));
        Set<OpenOption> optionsSet = Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        try {
            destination = AsynchronousFileChannel.open(fileName, optionsSet, executor);
            // Failed to used StandardOpenOption.(APPEND vs CREATE), so truncate it if needed after creation
            if (truncate) {
                destination.truncate(0);
            }
            position = new AtomicLong(destination.size());
        } catch (IOException | UnsupportedOperationException e) {
            logger.error("error opening output file {}: {}", () -> fileName, () -> Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
        try {
            byte[] msg = getEncoder().encode(event);
            ByteBuffer buffer = ByteBuffer.allocate(msg.length + separatorBytes.length) ;
            buffer.put(msg);
            buffer.put(separatorBytes);
            buffer.flip();
            long writepos = position.getAndAdd((long)msg.length
                                                + separatorBytes.length);
            destination.write(buffer, writepos, event, handler);
            return true;
        } catch (IllegalArgumentException | NonWritableChannelException e) {
            logger.error("error writing event to {}: {}", fileName,
                         Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return false;
        }
    }

    @Override
    protected void flush(Batch batch) {
        batch.forEach(ev -> {
            try {
                boolean status = send(ev.getEvent());
                ev.complete(status);
            } catch (SendException | EncodeException ex) {
                ev.completeExceptionally(ex);
            }
        });
    }

   @Override
    public void customStopSending() {
        if (destination.isOpen()) {
            try {
                // The lock will not be released, as you don't release a closed file
                destination.lock();
                destination.force(true);
                destination.close();
            } catch (IOException e) {
                logger.error("Failed to close {}: {}", () -> fileName, () -> Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } 
        }
    }

    @Override
    public String getSenderName() {
        return "File_" + fileName;
    }

}
