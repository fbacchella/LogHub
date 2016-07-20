package loghub;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class Stats {
    public final static AtomicLong received = new AtomicLong();
    public final static AtomicLong dropped = new AtomicLong();
    public final static AtomicLong sent = new AtomicLong();
    public final static AtomicLong failed = new AtomicLong();
    public final static AtomicLong thrown = new AtomicLong();

    private final static Queue<ProcessorException> errors = new ArrayBlockingQueue<>(100);
    private final static Queue<Exception> exceptions = new ArrayBlockingQueue<>(100);

    private Stats() {
    }

    public static synchronized void newError(ProcessorException e) {
        failed.incrementAndGet();
        try {
            errors.add(e);
        } catch (IllegalStateException ex) {
            errors.remove();
            errors.add(e);
        }
    }

    public static synchronized void newException(Exception e) {
        thrown.incrementAndGet();
        try {
            exceptions.add(e);
        } catch (IllegalStateException ex) {
            exceptions.remove();
            exceptions.add(e);
        }
    }

    public static synchronized List<ProcessorException> getErrors() {
        return new ArrayList<>(errors);
    }

    public static List<Exception> getExceptions() {
        return new ArrayList<>(exceptions);
    }

}
