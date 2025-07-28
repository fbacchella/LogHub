package loghub.cbor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public final class CborTagHandlerService {

    private static final Map<Integer, CborTagHandler<?>> handlersByTag;
    private static final Map<Class<?>, CborTagHandler<?>> handlersByType;

    public interface CustomParser<T> {
        default boolean usable(CBORParser p) {
            return true;
        }
        T parse(CBORParser p) throws IOException;
    }

    public interface CustomWriter<T> {
        default boolean usable(T data, CBORGenerator pp) {
            return true;
        }
        void write(T data, CBORGenerator p) throws IOException;
    }

    static {
        ServiceLoader<CborTagHandler> loader = ServiceLoader.load(CborTagHandler.class);
        Map<Integer, CborTagHandler<?>> tempTags = new HashMap<>();
        Map<Class<?>, CborTagHandler<?>> tempTypes = new HashMap<>();
        for (CborTagHandler<?> handler : loader) {
            tempTags.put(handler.getTag(), handler);
            for (Class<?> type : handler.getTargetTypes()) {
                tempTypes.put(type, handler);
            }
        }
        handlersByTag = Map.copyOf(tempTags);
        handlersByType = Map.copyOf(tempTypes);
    }

    private CborTagHandlerService() {
        // Classe utilitaire, pas d'instance
    }

    public static Optional<CborTagHandler<?>> getByTag(int tag) {
        return Optional.ofNullable(handlersByTag.get(tag));
    }

    public static Optional<CborTagHandler<?>> getByType(Class<?> clazz) {
        return Optional.ofNullable(handlersByType.get(clazz));
    }

    public static Collection<CborTagHandler<?>> allHandlers() {
        return handlersByTag.values();
    }

    public static Collection<Class<?>> allHandledClasses() {
        return handlersByType.keySet();
    }

}
