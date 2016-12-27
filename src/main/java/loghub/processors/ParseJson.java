package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Event;
import loghub.ProcessorException;

public class ParseJson extends FieldsProcessor {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        String message = event.get(field).toString();
        try {
            Object o = json.get().readValue(new StringReader(message), Object.class);
            if(o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                map.entrySet().stream().forEach( (i) -> event.put(i.getKey().toString(), i.getValue()));
            } else {
                event.put(destination, o);
            }
            return true;
        } catch (IOException e) {
            throw event.buildException("failed to parse json " + message, e);
        }
    }

    @Override
    public String getName() {
        return "ToJson";
    }
}
