package loghub.events;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

import com.codahale.metrics.Timer;

import loghub.metrics.Stats;

public class EventFinalizer extends PhantomReference<Event> {

    private final Runnable clean;
    private volatile boolean ended = false;

    public EventFinalizer(Event referent, ReferenceQueue<Event> referenceQueue, Timer.Context timer) {
        super(referent, referenceQueue);
        this.clean = () -> EventsFactory.finishEvent(true, timer);
    }

    @Override
    public void clear() {
        super.clear();
        if (ended) {
            Stats.duplicateEnd();
            // To detect duplication during tests
            assert false : "Duplicate end of event";
        }
        ended = true;
    }

    public void finalizeResources() {
        // free resources
        clean.run();
        Stats.eventLeaked();
        // To detect leaks during tests.
        assert false : "Leaked event";
    }

}
