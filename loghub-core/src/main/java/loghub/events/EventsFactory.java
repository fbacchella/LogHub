package loghub.events;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;

import loghub.ConnectionContext;
import loghub.Pipeline;

public class EventsFactory {

    private static final Logger logger = LogManager.getLogger();

    final ReferenceQueue<Event> referenceQueue = new ReferenceQueue<>();
    private final Map<Pipeline, PreSubPipline> preSubpiplines = new ConcurrentHashMap<>();

    PreSubPipline getPreSubPipeline(Pipeline pipe) {
        return preSubpiplines.computeIfAbsent(pipe, PreSubPipline::new);
    }

    public Event newTestEvent() {
        return new EventInstance(ConnectionContext.EMPTY, true, this);
    }
    public Event newTestEvent(ConnectionContext<?> ipctx) {
        return new EventInstance(ipctx, true, this);
    }

    public Event newEvent() {
        leakDetector();
        return new EventInstance(ConnectionContext.EMPTY, this);
    }

    public Event newEvent(ConnectionContext<?> ctx) {
        leakDetector();
        return new EventInstance(ctx, this);
    }

    public static void deadEvent(ConnectionContext<?> ctx) {
        new EventInstance(ConnectionContext.EMPTY, null).end();
    }

    private void leakDetector() {
        Reference<?> referenceFromQueue;
        while ((referenceFromQueue = referenceQueue.poll()) != null) {
            ((EventFinalizer) referenceFromQueue).finalizeResources();
        }
    }

    static void finishEvent(boolean leak, Timer.Context timer) {
        timer.close();
        if (leak) {
            logger.error("Event leaked");
        }
    }

}
