package loghub.events;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.ConnectionContext;
import loghub.Pipeline;

public class EventsFactory {

    private final Map<Pipeline, PreSubPipline> preSubpiplines = new ConcurrentHashMap<>();

    PreSubPipline getPreSubPipeline(Pipeline pipe) {
        return preSubpiplines.computeIfAbsent(pipe, PreSubPipline::new);
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
