package loghub.processors;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectReader;

import loghub.Event;
import loghub.ProcessorException;
import loghub.jackson.JacksonBuilder;
import lombok.Getter;
import lombok.Setter;

public class ParseJson extends FieldsProcessor {

    @Getter @Setter
    String atPrefix = "_";

    private final ObjectReader reader;
    
    public ParseJson() {
        reader = JacksonBuilder.get()
                .setFactory(new JsonFactory())
                .getReader();
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            Object o = reader.readValue(value.toString());
            if (o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                for(Map.Entry<Object, Object> e: map.entrySet()) {
                    String key = e.getKey().toString();
                    if (key.startsWith("@")) {
                        event.put(atPrefix + key.substring(1), e.getValue());
                    } else {
                        event.put( key, e.getValue());
                    }
                }
                return FieldsProcessor.RUNSTATUS.NOSTORE;
            } else {
                return o;
            }
        } catch (IOException e) {
            throw event.buildException("failed to parse json " + value, e);
        }
    }

    @Override
    public String getName() {
        return "ToJson";
    }

}
