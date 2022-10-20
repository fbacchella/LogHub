package loghub.events;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import loghub.ConnectionContext;

public class EventsFactory {

    private static final Class[] EVENT_CLASSES = new Class[] { EventWrapper.class, EventInstance.class, Event.class};
    public static Iterable<Class> getEventClasses() {
        return () -> Arrays.stream(EVENT_CLASSES).iterator();
    }

    private final Map<String, PreSubpipline> preSubpiplines = new ConcurrentHashMap<>();

    PreSubpipline getPre(String name) {
        return preSubpiplines.computeIfAbsent(name, PreSubpipline::new);
    }

    public Event newTestEvent() {
        return new EventInstance(ConnectionContext.EMPTY, true,this);
    }
    public Event newTestEvent(ConnectionContext<?> ipctx) {
        return new EventInstance(ipctx, true,this);
    }

    public Event newEvent() {
        return new EventInstance(ConnectionContext.EMPTY, this);
    }

    public Event newEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx, this);
    }
    public static void deadEvent(ConnectionContext<?> ctx) {
        new EventInstance(ConnectionContext.EMPTY, null).end();
    }

}
