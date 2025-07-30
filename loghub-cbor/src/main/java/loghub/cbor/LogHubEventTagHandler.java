package loghub.cbor;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

import loghub.cbor.CborTagHandlerService.CustomParser;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class LogHubEventTagHandler extends CborTagHandler<Event> {

    public static final int EVENT_TAG = 65535;
    public static final String EVENT_VERSION = "1.0";

    public LogHubEventTagHandler() {
        super(EVENT_TAG, Event.class);
    }

    @Override
    public Event parse(CborParser p) throws IOException {
        throw new IllegalArgumentException("Needs a custom parser");
    }

    @Override
    public void write(Event event, CBORGenerator p) throws IOException {
        Event oldEvent;
        do {
            oldEvent = event;
            event = event.unwrap();
        } while (oldEvent != event);
        event = event.unwrap();
        p.writeStartArray();
        p.writeString(EVENT_VERSION);
        p.writeObject(event.getTimestamp());
        p.writeObject(event.getMetas());
        p.writeObject(new HashMap<>(event));
        p.writeEndArray();
    }

    public static CustomParser<Event> eventParser(EventsFactory factory) {
        return p -> {
            Event ev = factory.newEvent();
            JsonToken token = p.nextToken();
            assert token == JsonToken.VALUE_STRING;
            String version = p.readText();
            assert EVENT_VERSION.equals(version);
            token = p.nextToken();
            assert token == JsonToken.VALUE_NUMBER_FLOAT;
            Instant ts = p.readValue();
            ev.setTimestamp(ts);
            token = p.nextToken();
            assert token == JsonToken.START_OBJECT;
            Map<String, Object> metas = p.readValue();
            ev.getMetas().putAll(metas);
            token = p.nextToken();
            assert token == JsonToken.START_OBJECT;
            Map<String, Object> data = p.readValue();
            token = p.nextToken();
            assert token == JsonToken.END_ARRAY;
            ev.putAll(data);
            return ev;
        };
    }

}
