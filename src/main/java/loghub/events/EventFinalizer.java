package loghub.events;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

import com.codahale.metrics.Timer;

import loghub.metrics.Stats;

public class EventFinalizer extends PhantomReference<Event> {

    private final Runnable clean;

    public EventFinalizer(Event referent, ReferenceQueue<Event> referenceQueue, Timer.Context timer) {
        super(referent, referenceQueue);
        this.clean = () -> EventsFactory.finishEvent(true, timer);
    }

    public void finalizeResources() {
        // free resources
        clean.run();
        Stats.eventLeaked();
    }



}
