package loghub.cbor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class CborTagHandlerService {

    private final Map<Integer, CborTagHandler<Object>> handlersByTag;
    private final Map<Class<Object>, CborTagHandler<Object>> handlersByType;

    public interface CustomParser<T> {
        @SuppressWarnings("unused")
        default boolean usable(CBORParser p) {
            return true;
        }
        T parse(CBORParser p) throws IOException;
    }

    public interface CustomWriter<T> {
        @SuppressWarnings("unused")
        default boolean usable(T data, CBORGenerator pp) {
            return true;
        }
        void write(T data, CBORGenerator p) throws IOException;
    }

    @SuppressWarnings({"unchecked", "java:S3740", "rawtypes"})
    private CborTagHandlerService(ServiceLoader<CborTagHandler> loader) {
        Map<Integer, CborTagHandler<Object>> tempTags = new HashMap<>();
        Map<Class<Object>, CborTagHandler<Object>> tempTypes = new HashMap<>();
        for (CborTagHandler<Object> handler : loader) {
            tempTags.put(handler.getTag(), handler);
            for (Class<?> type : handler.getTargetTypes()) {
                tempTypes.put((Class<Object>)type, handler);
            }
        }
        handlersByTag = Map.copyOf(tempTags);
        handlersByType = Map.copyOf(tempTypes);
    }

    public CborTagHandlerService() {
        this(ServiceLoader.load(CborTagHandler.class));
    }

    public CborTagHandlerService(ClassLoader clLoader) {
        this(ServiceLoader.load(CborTagHandler.class, clLoader));
    }

   public Optional<CborTagHandler<Object>> getByTag(int tag) {
        return Optional.ofNullable(handlersByTag.get(tag));
    }

    public Optional<CborTagHandler<Object>> getByType(Class<?> clazz) {
        return Optional.ofNullable(handlersByType.get(clazz));
    }

    public Collection<CborTagHandler<Object>> allHandlers() {
        return handlersByTag.values();
    }

    public Collection<Class<Object>> allHandledClasses() {
        return handlersByType.keySet();
    }

    public Stream<CborSerializer<Object>> makeSerializers() {
        return handlersByType.entrySet().stream().map(e -> new CborSerializer<>(e.getValue(), e.getKey()));
    }

}
