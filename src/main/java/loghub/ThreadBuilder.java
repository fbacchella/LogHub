package loghub;

import java.util.concurrent.FutureTask;

public class ThreadBuilder<T> {
    public static <T> ThreadBuilder<T> get(Class<T> clazz) {
        return new ThreadBuilder<T>();
    }

    public static ThreadBuilder<Object> get() {
        return new ThreadBuilder<Object>();
    }

    private Runnable task;
    private String name = null;
    private Boolean daemon = null;
    private boolean shutdownHook = false;

    private ThreadBuilder() {
    }

    public ThreadBuilder<T> setName(String name) {
        this.name = name;
        return this;
    }

    public ThreadBuilder<T> setDaemon(boolean on) {
        return this;
    }

    public ThreadBuilder<T> setCallable(FutureTask<T> task) {
        this.task = task;
        return this;
    }

    public ThreadBuilder<T> setRunnable(Runnable r) {
        task = r;
        return this;
    }

    public ThreadBuilder<T> setShutdownHook(boolean shutdownHook) {
        this.shutdownHook = shutdownHook;
        return this;
    }

    public Thread build() {
        return build(false);
    }

    public Thread build(boolean start) {
        if (shutdownHook && start) {
            throw new IllegalArgumentException("A thread can't be both to be started and shutdown hook");
        }
        Thread t = new Thread(task);
        if (daemon != null) t.setDaemon(daemon);
        if (name != null) t.setName(name);
        if (shutdownHook) Runtime.getRuntime().addShutdownHook(t);
        if (start) t.start();
        return t;
    }
}
