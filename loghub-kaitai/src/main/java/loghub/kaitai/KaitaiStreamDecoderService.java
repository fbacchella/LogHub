package loghub.kaitai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;

/**
 * A decoding service for Kaitai structures.
 * @param <T> The KaitaiStruct subclass handled by this service.
 */
public abstract class KaitaiStreamDecoderService<T extends KaitaiStruct> {

    /**
     * Decode an instance of the Kaitai structure.
     * @param struct The structure instance to decode.
     * @return An optional object resulting from the decoding.
     */
    public abstract Optional<? extends KaitaiStruct> decode(T struct, KaitaiStream stream);

    /**
     * Return the class of the Kaitai structure this service can handle.
     * @return The structure class.
     */
    public abstract Class<T> getKind();

    private static final Map<Class<? extends KaitaiStruct>, List<KaitaiStreamDecoderService<?>>> services = new HashMap<>();
    static {
        loadServices();
    }

    private static synchronized void loadServices() {
        ServiceLoader<KaitaiStreamDecoderService> loader = ServiceLoader.load(KaitaiStreamDecoderService.class);
        Map<Class<? extends KaitaiStruct>, List<KaitaiStreamDecoderService<?>>> tmp = loader.stream()
                         .map(p -> (KaitaiStreamDecoderService<?>) p.get())
                         .collect(java.util.stream.Collectors.groupingBy(KaitaiStreamDecoderService::getKind));
        services.clear();
        services.putAll(tmp);
    }

    /**
     * Reload the decoding services.
     */
    public static void reset() {
        loadServices();
    }

    public static Optional<? extends KaitaiStruct> resolve(KaitaiStruct packet, KaitaiStream stream) {
        List<KaitaiStreamDecoderService<?>> serviceList = services.getOrDefault(packet.getClass(), List.of());
        for (KaitaiStreamDecoderService<?> service : serviceList) {
            @SuppressWarnings("unchecked")
            KaitaiStreamDecoderService<KaitaiStruct> typedService = (KaitaiStreamDecoderService<KaitaiStruct>) service;
            Optional<? extends KaitaiStruct> result = typedService.decode(packet, stream);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
}
