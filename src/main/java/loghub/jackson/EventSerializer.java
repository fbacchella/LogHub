package loghub.jackson;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import loghub.Event;

public class EventSerializer extends JsonSerializer<Event> {
    @Override
    public void serialize(Event value, JsonGenerator gen,
                          SerializerProvider serializers)
                                          throws IOException {
        Map<String, Object> eventContent = new HashMap<>();
        Map<String, Object> eventFields = new HashMap<>();
        Map<String, Object> eventMetas = new HashMap<>();
        eventFields.putAll(value);
        value.getMetaAsStream().forEach( i-> eventMetas.put(i.getKey(), i.getValue()));
        eventContent.put("@fields", eventFields);
        eventContent.put(Event.TIMESTAMPKEY, value.getTimestamp());
        eventContent.put("@METAS", eventMetas);
        Map<String, Object> eventMap = Collections.singletonMap(Event.class.getCanonicalName(), eventContent);
        gen.writeObject(eventMap);
    }
    @Override
    public Class<Event> handledType() {
        return Event.class;
    }

}
