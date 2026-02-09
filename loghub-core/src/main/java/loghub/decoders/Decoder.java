package loghub.decoders;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import io.netty.buffer.ByteBuf;
import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventBuilder;
import loghub.events.EventsFactory;
import loghub.receivers.Receiver;
import lombok.Getter;
import lombok.Setter;

public abstract class Decoder {

    @Setter
    public abstract static class Builder<B extends Decoder> extends AbstractBuilder<B> {
        private String field = "message";
    }

    @FunctionalInterface
    public interface ObjectDecoder {
        Object get() throws DecodeException;
    }

    private static final StackLocator stacklocator = StackLocator.getInstance();

    protected final Logger logger;

    @Getter
    protected final String field;
    private Receiver<?, ?> receiver;

    protected Decoder(Builder<?  extends Decoder> builder) {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
        field = builder.field;
    }

    public boolean configure(Properties properties, Receiver<?, ?> receiver) {
        this.receiver = receiver;
        return true;
    }

    protected void manageDecodeException(ConnectionContext<?> connectionContext, DecodeException ex) {
        receiver.manageDecodeException(ex);
        EventsFactory.deadEvent(connectionContext);
    }

    protected Object decodeObject(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return decodeObject(ctx, msg, 0, msg.length);
    }

    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuffer bbuf) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    private Stream<Map<String, Object>> resolve(ConnectionContext<?> ctx, Object o)  {
        return resolveToStream(o).filter(Objects::nonNull)
                                 .flatMap(getDecodeMap(ctx))
                                 .filter(Objects::nonNull);
    }

    private Function<Object, Stream<Map<String, Object>>> getDecodeMap(ConnectionContext<?> ctx) {
        return m -> resolveToStream(m).map(i -> decodeMap(ctx, i));
    }

    private Stream<?> resolveToStream(Object source) {
        return switch (source) {
            case null -> Stream.empty();
            case Collection<?> c -> c.stream();
            case Stream<?> s -> s;
            case Iterable<?> i -> StreamSupport.stream(i.spliterator(), false);
            case Iterator<?> i ->
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(i, Spliterator.ORDERED), false);
            default -> Stream.of(source);
        };
    }

    private Map<String, Object> decodeMap(ConnectionContext<?> ctx, Object o) {
        return switch (o) {
        case Event ev -> ev;
        case EventBuilder evb -> evb.setContext(ctx).getInstance();
        case Map<?, ?> map1 -> {
            Map<String, Object> newMap = HashMap.newHashMap(map1.size());
            map1.forEach((key, value) -> newMap.put(key.toString(), value));
            yield newMap;
        }
        default -> {
            if (field != null) {
                yield Collections.singletonMap(field, o);
            } else {
                manageDecodeException(ctx, new DecodeException("Can't be mapped to event"));
                yield null;
            }
        }
        };
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return parseObjectStream(ctx, () -> decodeObject(ctx, bbuf));
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, ByteBuffer bbuf) throws DecodeException {
        return parseObjectStream(ctx, () -> decodeObject(ctx, bbuf));
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return parseObjectStream(ctx, () -> decodeObject(ctx, msg, offset, length));
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return parseObjectStream(ctx, () -> decodeObject(ctx, msg));
    }

    protected final Stream<Map<String, Object>> parseObjectStream(ConnectionContext<?> ctx, ObjectDecoder objectsSource) throws DecodeException {
        return resolve(ctx, objectsSource.get());
    }

}
