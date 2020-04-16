package loghub;

import java.util.concurrent.FutureTask;
import java.util.function.BiConsumer;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true) @Setter
public class ThreadBuilder {
    
    private static class ThreadCustomInterrupt extends Thread {
        private final BiConsumer<Thread, Runnable> interruptHandler;
        volatile boolean interrupted;
        ThreadCustomInterrupt(Runnable task, BiConsumer<Thread, Runnable> interruptHandler) {
            super(task);
            this.interruptHandler = interruptHandler;
            interrupted = false;
        }
        @Override
        public void interrupt() {
            interrupted = true;
            interruptHandler.accept(this, super::interrupt);
        }
        @Override
        public boolean isInterrupted() {
            return interrupted;
        }
    }

    public static ThreadBuilder get() {
        return new ThreadBuilder();
    }

    private Runnable task;
    private BiConsumer<Thread, Runnable> interrupter = null;
    private String name = null;
    private Boolean daemon = null;
    private boolean shutdownHook = false;
    private Thread.UncaughtExceptionHandler exceptionHander =  null;

    private ThreadBuilder() {
    }

    public ThreadBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public ThreadBuilder setDaemon(boolean on) {
        return this;
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
        if (interrupter == null) {
            t = new Thread(task);
        } else {
            t = new ThreadCustomInterrupt(task, interrupter);
        }
        if (daemon != null) t.setDaemon(daemon);
        if (name != null) t.setName(name);
        if (shutdownHook) Runtime.getRuntime().addShutdownHook(t);
        if (exceptionHander != null) t.setUncaughtExceptionHandler(exceptionHander);
        if (start) t.start();
        return t;
    }
}
