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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;

import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.metrics.Stats;
import lombok.Getter;
import lombok.Setter;

@AsyncSender
@CanBatch
@BuilderClass(File.Builder.class)
public class File extends Sender {

    public static class Builder extends Sender.Builder<File> {
        @Setter
        private Expression fileName;
        @Setter
        private String separator = "";
        @Setter
        private boolean truncate = false;
        @Setter
        private int cacheSize = 10;
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

    private static class FileEventListener implements CacheEntryRemovedListener<Path,FileEntry> {
        @Override
        public void onRemoved(
                Iterable<CacheEntryEvent<? extends Path, ? extends FileEntry>> cacheEntryEvents)
                throws CacheEntryListenerException {
            cacheEntryEvents.forEach(e -> {
                try {
                    e.getValue().close();
                } catch (IOException ex) {
                    throw new CacheEntryListenerException(ex);
                }
            });
        }
    }

    private static final Set<OpenOption> OPTIONS_SET = Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE);

    public class FileEntry {
        private final AsynchronousFileChannel destination;
        private final AtomicLong position;

        public FileEntry(Path destination) throws IOException {
            this.destination = AsynchronousFileChannel.open(destination, OPTIONS_SET, executor);
            // Failed to used StandardOpenOption.(APPEND vs CREATE), so truncate it if needed after creation
            if (truncate) {
                this.destination.truncate(0);
            }
            position = new AtomicLong(this.destination.size());
        }

        public void close() throws IOException {
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
    }

    @Getter
    private final Expression fileName;
    private final byte[] separatorBytes;
    private final boolean truncate;
    private final int cacheSize;
    private Cache<Path, FileEntry> filecache;
    private final ExecutorService executor;

    public File(Builder builder) {
        super(builder);
        if (! builder.separator.isEmpty()) {
            separatorBytes = builder.separator.getBytes(StandardCharsets.UTF_8);
        } else {
            separatorBytes = new byte[] {};
        }
        fileName = builder.fileName;
        truncate = builder.truncate;
        cacheSize = builder.cacheSize;
        executor = Executors.newCachedThreadPool(
                    ThreadBuilder.get()
                                 .setDaemon(true)
                                 .getFactory("AsyncIO")
        );
    }

    @Override
    public boolean configure(Properties properties) {
        Factory<? extends CacheEntryListener<Path, FileEntry>> listenerFactory = FileEventListener::new;
        CacheEntryListenerConfiguration<Path, FileEntry> config = new MutableCacheEntryListenerConfiguration<>(listenerFactory, null, true, true);
        filecache = properties.cacheManager.getBuilder(
                        Path.class, FileEntry.class)
                            .setName("FileCache", this)
                            .setCacheSize(cacheSize)
                            .addListenerConfiguration(config)
                            .storeByValue(false)
                            .setCacheLoaderFactory(() -> new CacheLoader<>() {
                                @Override
                                public FileEntry load(Path key) throws CacheLoaderException {
                                    try {
                                        return new FileEntry(key);
                                    } catch (IOException e) {
                                        throw new CacheLoaderException(e);
                                    }
                                }

                                @Override
                                public Map<Path, FileEntry> loadAll(Iterable<? extends Path> keys)
                                        throws CacheLoaderException {
                                    throw new IllegalAccessError("Not implemented");
                                }
                            })
                            .build();
        return super.configure(properties);
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
        logger.trace("Sending event {}", event);
        try {
            byte[] msg = getEncoder().encode(event);
            ByteBuffer buffer = ByteBuffer.allocate(msg.length + separatorBytes.length) ;
            buffer.put(msg);
            buffer.put(separatorBytes);
            buffer.flip();
            Path outputFile = Paths.get(fileName.eval(event).toString());
            FileEntry fe = filecache.get(outputFile);
            long writePosition = fe.position.getAndAdd((long)msg.length + separatorBytes.length);
            fe.destination.write(buffer, writePosition, event, handler);
            Stats.sentBytes(this, buffer.limit());
            return true;
        } catch (CacheException ex) {
            Throwable rootCause = ex;
            while (rootCause instanceof CacheException && rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            handleException(rootCause, event);
            return false;
        } catch (ProcessorException | IllegalArgumentException | NonWritableChannelException ex) {
            handleException(ex, event);
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
        filecache.iterator().forEachRemaining(e -> {
            try {
                e.getValue().destination.force(false);
            } catch (IOException ex) {
                logger.warn("Can't flush {}: {}", e.getKey(), Helpers.resolveThrowableException(ex));
            }
        });
    }

   @Override
    public void customStopSending() {
       filecache.removeAll();
    }

    @Override
    public String getSenderName() {
        return "File_" + fileName;
    }

}
