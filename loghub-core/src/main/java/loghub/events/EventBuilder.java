package loghub.events;

import java.time.Instant;
import java.util.Map;

import loghub.ConnectionContext;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(chain = true)
public class EventBuilder {
    private Instant timestamp = Instant.now();
    private Map<String, Object> metas = Map.of();
    private Map<String, Object> data = Map.of();
    private ConnectionContext<?> context = ConnectionContext.EMPTY;
    private boolean test = false;
    private EventsFactory factory;

    public Event getInstance() {
        Event ev = factory.newEvent(context, test);
        ev.setTimestamp(timestamp);
        ev.getMetas().putAll(metas);
        ev.putAll(data);
        return ev;
    }

}
