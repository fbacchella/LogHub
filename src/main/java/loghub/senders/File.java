package loghub.senders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Properties;

public class File extends Sender {

    private final CompletionHandler<Integer, Object> handler = new CompletionHandler<Integer, Object>() {

        @Override
        public void completed(Integer result, Object attachment) {
        }

        @Override
        public void failed(Throwable exc, Object attachment) {
            String message = exc.getMessage();
            if (message == null) {
                message = exc.getClass().getSimpleName();
            }
            logger.error("error writing event to {}",fileName, message);
        }

    };
    private String fileName;

    private AsynchronousFileChannel destination;
    private AtomicLong position;

    public File(BlockingQueue<Event> inQueue) {
        super(inQueue);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            destination = AsynchronousFileChannel.open(Paths.get(fileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            try(FileLock l = destination.lock().get()) {
                position = new AtomicLong(l.position());
            }
        } catch (InterruptedException | ExecutionException | IOException | UnsupportedOperationException e) {
            String message = e.getMessage();
            if (message == null) {
                message = e.getClass().getSimpleName();
            }
            logger.error("error openening output file {}: {}", fileName, message);
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean send(Event event) {
        try {
            byte[] msg = getEncoder().encode(event);
            long writepose = position.getAndAdd(msg.length);
            destination.write(ByteBuffer.wrap(msg), writepose, null, handler);
            return true;
        } catch (IllegalArgumentException | NonWritableChannelException e) {
            logger.error("error writing event to {}: {}", fileName, e.getMessage());
            return false;
        }
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

}
