package loghub.events;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import com.codahale.metrics.Timer;

import loghub.ThreadBuilder;
import loghub.metrics.Stats;

public class EventFinalizer extends PhantomReference<Event> {

    static ReferenceQueue<Event> referenceQueue = new ReferenceQueue<>();
    static {
        ThreadBuilder.get()
                     .setDaemon(true)
                     .setName("EventLeakDetector")
                     .setTask(EventFinalizer::cleaner)
                     .build(true);
    }

    private static void cleaner() {
        Reference<?> referenceFromQueue;
        try {
            while ((referenceFromQueue = referenceQueue.remove()) != null) {
                ((EventFinalizer)referenceFromQueue).finalizeResources();
                referenceFromQueue.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Game over
        }
    }

    Runnable clean;
    public EventFinalizer(Event referent, Timer.Context timer, Queue<ExecutionStackElement> executionStack) {
        super(referent, referenceQueue);
        this.clean = () -> EventInstance.finish(true, timer, executionStack);
    }

    public void finalizeResources() {
        // free resources
        clean.run();
        Stats.eventLeaked();
    }

}
