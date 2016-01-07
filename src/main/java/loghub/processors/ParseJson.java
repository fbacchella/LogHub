package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Event;
import loghub.Processor;

public class ParseJson extends Processor {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    private String field = "message";

    @Override
    public void process(Event event) {
        try {
            Object o = json.get().readValue(new StringReader(event.get(field).toString()), Object.class);
            if(o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                map.entrySet().stream().forEach( (i) -> addElement(event, i.getKey().toString(), i.getValue()));
            } else {
                addElement(event, field, o);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String getName() {
        return "ToJson";
    }
}
