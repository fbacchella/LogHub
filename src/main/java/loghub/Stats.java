package loghub;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class Stats {
    public final static AtomicLong received = new AtomicLong();
    public final static AtomicLong dropped = new AtomicLong();
    public final static AtomicLong sent = new AtomicLong();
    public final static AtomicLong failed = new AtomicLong();
    public final static AtomicLong thrown = new AtomicLong();

    private final static Queue<ProcessingException> errors = new ArrayBlockingQueue<>(100);
    private final static Queue<Exception> exceptions = new ArrayBlockingQueue<>(100);

    private Stats() {
    }

    public static synchronized void reset() {
        errors.clear();
        exceptions.clear();
        received.set(0);
        dropped.set(0);
        sent.set(0);
        failed.set(0);
        thrown.set(0);
    }

    public static synchronized void newError(ProcessingException e) {
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

    public static synchronized Collection<ProcessingException> getErrors() {
        return Collections.unmodifiableCollection(errors);
    }

    public static Collection<Exception> getExceptions() {
        return Collections.unmodifiableCollection(exceptions);
    }

}
