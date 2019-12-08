package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import loghub.Event;
import loghub.ProcessorException;
import lombok.Getter;
import lombok.Setter;

public class ParseJson extends FieldsProcessor {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            ObjectMapper mapper = new ObjectMapper(factory);
            mapper.setDefaultTyping(StdTypeResolverBuilder.noTypeInfoBuilder());
            return mapper;
        }
    };

    @Getter @Setter
    String atPrefix = "_";

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            Object o = json.get().readValue(new StringReader(value.toString()), Object.class);
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
