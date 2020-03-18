package loghub.decoders;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.decoders.Decoder.DecodeException;
import lombok.Getter;

public class JacksonDeserializer {

    @FunctionalInterface
    public static interface ObjectResolver {
        Object deserialize(ObjectMapper om) throws DecodeException, IOException;
    }

    @Getter
    private final ObjectMapper mapper;

    public JacksonDeserializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, ObjectResolver gen) throws DecodeException, IOException {
        Object o = gen.deserialize(mapper);
        if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) o;
            return Stream.of(decodeMap(ctx, map));
        } else if (o instanceof Collection){
            @SuppressWarnings("unchecked")
            Collection<Object> coll = (Collection<Object>) o;
            return subMapper(ctx, coll.stream());
        } else if (o instanceof Iterable){
            @SuppressWarnings("unchecked")
            Iterable<Object> i = (Iterable<Object>) o;
            return subMapper(ctx, StreamSupport.stream(i.spliterator(), false));
        }  else {
            throw new Decoder.DecodeException("Can't be mapped to event");
        }
    }

    private Stream<Map<String, Object>> subMapper(ConnectionContext<?> ctx, Stream<Object> s) throws DecodeException {
        Function<Object, Map<String, Object>> sm =  m -> {
            try {
                return decodeMap(ctx, m);
            } catch (DecodeException e) {
                throw new Decoder.RuntimeDecodeException(e);
            }
        };
        return s.map(sm);
    }

    private Map<String, Object> decodeMap(ConnectionContext<?> ctx, Object o) throws DecodeException {
        if (!(o instanceof Map)) {
            throw new Decoder.DecodeException("Can't be mapped to event");
        }

        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) o;

        if (map.size() == 1 && map.containsKey(Event.class.getCanonicalName())) {
            // Special case, the message contain a loghub event, sent from another loghub
            @SuppressWarnings("unchecked")
            Map<String, Object> eventContent = (Map<String, Object>) map.remove(Event.class.getCanonicalName());
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>)eventContent.remove("@fields");
            @SuppressWarnings("unchecked")
            Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
            Event newEvent = Event.emptyEvent(ctx);
            newEvent.putAll(fields);
            Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
            .filter(newEvent::setTimestamp)
            .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
            metas.forEach((i,j) -> newEvent.putMeta(i, j));
            return newEvent;
        } else {
            Map<String, Object> newMap = new HashMap<>(map.size());
            map.entrySet().stream().forEach((i) -> newMap.put(i.getKey().toString(), i.getValue()));
            return newMap;
        }
    }

}
