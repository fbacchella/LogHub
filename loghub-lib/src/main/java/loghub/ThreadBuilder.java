package loghub;

import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Setter
public class ThreadBuilder {

    private static class ThreadCustomInterrupt extends Thread {
        private final BiConsumer<Thread, Runnable> interruptHandler;
        private volatile boolean isInterrupted;
        ThreadCustomInterrupt(Runnable task, BiConsumer<Thread, Runnable> interruptHandler) {
            super(task);
            this.interruptHandler = interruptHandler;
            isInterrupted = false;
        }
        @Override
        public void interrupt() {
            isInterrupted = true;
            interruptHandler.accept(this, super::interrupt);
        }
        @Override
        public boolean isInterrupted() {
            return isInterrupted;
        }
    }

    public static ThreadBuilder get() {
        return new ThreadBuilder();
    }

    @Setter
    private static Thread.UncaughtExceptionHandler defaultuncaughtexceptionhandler = null;

    private Runnable task;
    private BiConsumer<Thread, Runnable> interrupter = null;
    private String name = null;
    private Boolean daemon = null;
    private boolean shutdownHook = false;
    private Thread.UncaughtExceptionHandler exceptionHandler = defaultuncaughtexceptionhandler;
    private ClassLoader contextClassLoader = null;
    private boolean virtual = false;

    private ThreadFactory factory = null;

    private ThreadBuilder() {
    }

    public ThreadBuilder setCallable(FutureTask<?> task) {
        this.task = task;
        return this;
    }

    public Thread build() {
        return build(false);
    }

    public Thread build(boolean start) {
        if (shutdownHook && start) {
            throw new IllegalArgumentException("A thread can't be both started and being a shutdown hook");
        }
        Thread t;
        if (factory != null) {
            t = factory.newThread(task);
        } else if (virtual) {
            t = Thread.ofVirtual().unstarted(task);
        } else if (interrupter == null) {
            t = new Thread(task);
        } else {
            t = new ThreadCustomInterrupt(task, interrupter);
        }
        if (daemon != null) t.setDaemon(daemon);
        if (name != null) t.setName(name);
        if (shutdownHook) Runtime.getRuntime().addShutdownHook(t);
        if (exceptionHandler != null) t.setUncaughtExceptionHandler(exceptionHandler);
        if (contextClassLoader != null) t.setContextClassLoader(contextClassLoader);
        if (start) t.start();
        return t;
    }

    public ThreadFactory getFactory(String prefix) {
        AtomicInteger threadCount = new AtomicInteger(0);
        // A local ThreadBuilder, so the original ThreadBuilder can be reused
        ThreadBuilder newBuilder = new ThreadBuilder();
        newBuilder.task = null;
        newBuilder.interrupter = interrupter;
        newBuilder.name = null;
        newBuilder.daemon = daemon;
        newBuilder.shutdownHook = shutdownHook;
        newBuilder.exceptionHandler = exceptionHandler;
        newBuilder.factory = factory;
        newBuilder.contextClassLoader = contextClassLoader;
        Supplier<String> formatter = () -> prefix + "-" + threadCount.incrementAndGet();

        return r -> {
            // synchronized so the ThreadFactory is thread safe
            synchronized (newBuilder) {
                Thread t = newBuilder.setTask(r)
                                     .setName(formatter.get())
                                     .build();
                // Don’t hold references to the task or the name
                newBuilder.task = null;
                newBuilder.name = null;
                return t;
            }
        };
    }

}
