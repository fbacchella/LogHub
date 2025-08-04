package loghub.events;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;

import loghub.ConnectionContext;
import loghub.cloners.Immutable;
import loghub.Pipeline;
import loghub.decoders.DecodeException;

@Immutable
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

    public Event newEvent() {
        return newEvent(ConnectionContext.EMPTY, false);
    }

    public Event newEvent(ConnectionContext<?> ctx) {
        return newEvent(ctx, false);
    }

    public Event newEvent(ConnectionContext<?> ctx, boolean test) {
        leakDetector();
        if (ctx instanceof LockedConnectionContext lctx) {
            LockedConnectionContext newCtxt = (LockedConnectionContext) lctx.clone();
            return new EventInstance(newCtxt, test, this);
        } else {
            return new EventInstance(new LockedConnectionContext(ctx), test, this);
        }
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

    public Event mapToEvent(ConnectionContext<?> ctx, Map<String, Object> eventContent) throws DecodeException {
        return mapToEvent(ctx, eventContent, false);
    }

    public Event mapToEvent(ConnectionContext<?> ctx, Map<String, Object> eventContent, boolean test) throws DecodeException {
        if (eventContent instanceof Event event) {
            return event;
        } else if (eventContent.get("loghub.Event") instanceof Map) {
            return mapToEvent(ctx, (Map<String, Object>)eventContent.get("loghub.Event"), test);
        } else {
            if (! eventContent.containsKey("@fields") || ! eventContent.containsKey("@METAS")) {
                throw new DecodeException("Not a event map");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) eventContent.remove("@fields");
            @SuppressWarnings("unchecked")
            Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
            Event newEvent = newEvent(ctx, test);
            newEvent.putAll(fields);
            Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
                    .filter(newEvent::setTimestamp)
                    .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
            metas.forEach(newEvent::putMeta);
            return newEvent;
        }
    }

}
