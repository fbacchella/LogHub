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
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            Object o = json.get().readValue(new StringReader(value.toString()), Object.class);
            if (o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                map.entrySet().stream().forEach( (i) -> event.put(i.getKey().toString(), i.getValue()));
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
