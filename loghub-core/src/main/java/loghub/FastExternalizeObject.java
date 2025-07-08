package loghub;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.chrono.HijrahDate;
import java.time.chrono.JapaneseDate;
import java.time.chrono.MinguoDate;
import java.time.chrono.ThaiBuddhistDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import loghub.events.EventsFactory;
import lombok.Getter;

public class FastExternalizeObject {


    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Immutable {
    }

    private enum TYPE {
        NULL,
        TRUE,
        FALSE,
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        CHAR,
        DATE,
        ARRAY,
        MAP,
        LIST,
        EMPTY_CONTEXT,
        IMMUTABLE,
        FASTER,
        OTHER,
    }
    private static final TYPE[] TYPES = TYPE.values();

    private enum MAPCONSTRUCTOR {
        EMPTYMAP(n -> Map.of()),
        IDENTITYHASHMAP(n -> new IdentityHashMap<Object, Object>(n * 2)),
        CONCURRENTHASHMAP(n -> new ConcurrentHashMap<Object, Object>(n * 2)),
        HASHMAP(n -> new HashMap<Object, Object>(n * 2)),
        LINKEDHASHMAP(n -> new LinkedHashMap<Object, Object>(n * 2));

        private final Function<Integer, Map<Object, Object>> constructor;

        MAPCONSTRUCTOR(Function<Integer, Map<Object, Object>> constructor) {
            this.constructor = constructor;
        }

        Map<Object, Object> generate(int n) {
            return constructor.apply(n);
        }
    }
    private static final MAPCONSTRUCTOR[] MAPCONSTRUCTORS = MAPCONSTRUCTOR.values();

    private static final Map<Class<? extends Map>, MAPCONSTRUCTOR> MAP_MAPPING = Map.ofEntries(
            Map.entry(Map.of().getClass(), MAPCONSTRUCTOR.EMPTYMAP),
            Map.entry(Map.of(1, 2).getClass(), MAPCONSTRUCTOR.HASHMAP),
            Map.entry(IdentityHashMap.class, MAPCONSTRUCTOR.IDENTITYHASHMAP),
            Map.entry(ConcurrentHashMap.class, MAPCONSTRUCTOR.CONCURRENTHASHMAP),
            Map.entry(HashMap.class, MAPCONSTRUCTOR.HASHMAP),
            Map.entry(LinkedHashMap.class, MAPCONSTRUCTOR.LINKEDHASHMAP)
    );

    public static class FastObjectInputStream extends ObjectInputStream {

        @Getter
        private final EventsFactory factory;
        private final FastObjectOutputStream out;

        public FastObjectInputStream(byte[] buffer, EventsFactory factory, FastObjectOutputStream out) throws IOException {
            super(new ByteArrayInputStream(buffer));
            this.factory = factory;
            this.out = out;
        }

        private <K, V> Map<K, V> readMap() throws IOException, ClassNotFoundException {
            int constructor = readInt();
            int size = readInt();
            @SuppressWarnings("unchecked")
            Map<K, V> map = (Map<K, V>) MAPCONSTRUCTORS[constructor].generate(size);
            for (int i = 0; i < size; i++) {
                @SuppressWarnings("unchecked")
                K key = (K) readObjectFast();
                @SuppressWarnings("unchecked")
                V value = (V) readObjectFast();
                map.put(key, value);
            }
            return map;
        }

        public <K, V> Map<K, V> readMap(Map<K, V> map) throws IOException, ClassNotFoundException {
            int size = readInt();
            for (int i = 0; i < size; i++) {
                @SuppressWarnings("unchecked")
                K key = (K) readObjectFast();
                @SuppressWarnings("unchecked")
                V value = (V) readObjectFast();
                map.put(key, value);
            }
            return map;
        }

        public <E> List<E> readList() throws IOException, ClassNotFoundException {
            int size = readInt();
            List<E> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                @SuppressWarnings("unchecked")
                E e = (E) readObjectFast();
                list.add(e);
            }
            return list;
        }

        public Object readObjectFast() throws IOException, ClassNotFoundException {
            TYPE type = TYPES[read()];
            switch (type) {
            case NULL:
                return NullOrMissingValue.NULL;
            case TRUE:
                return true;
            case FALSE:
                return false;
            case BYTE:
                return readByte();
            case SHORT:
                return readShort();
            case INT:
                return readInt();
            case LONG:
                return readLong();
            case FLOAT:
                return readFloat();
            case DOUBLE:
                return readDouble();
            case CHAR:
                return readChar();
            case DATE:
                return new Date(readLong());
            case IMMUTABLE:
                return readReference();
            case ARRAY:
                return readArray();
            case MAP:
                return readMap();
            case LIST:
                return readList();
            case EMPTY_CONTEXT:
                return ConnectionContext.EMPTY;
            case FASTER:
                ObjectFaster<?> of = (ObjectFaster<?>) readObject();
                return of.get();
            default:
                return readObject();
            }
        }

        @SuppressWarnings("unchecked")
        private <T> T readReference() throws IOException {
            long ref = readLong();
            return (T) out.immutableObjectsCache.remove(ref);
        }

        @Override
        public String readUTF() throws IOException {
            return readReference();
        }

        public Object readArray() throws IOException, ClassNotFoundException {
            int length = readInt();
            Object array = readReference();
            for (int i = 0; i < length; i++) {
                Array.set(array, i, readObjectFast());
            }
            return array;
        }

        @Override
        public void close() throws IOException {
            if (! out.immutableObjectsCache.isEmpty()) {
                throw new IllegalStateException("Lost immutable objects: " + out.immutableObjectsCache);
            }
            super.close();
        }

    }

    public static class FastObjectOutputStream extends ObjectOutputStream {

        // Immutable objects can be reused, just forward a reference
        private final Map<Long, Object> immutableObjectsCache = new HashMap<>();
        private final AtomicLong ref = new AtomicLong(0);

        public FastObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        public void writeMap(Map<?, ?> map) throws IOException {
            writeInt(map.size());
            try {
                map.forEach((k, v) -> {
                    try {
                        writeObjectFast(k);
                        writeObjectFast(v);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            } catch (UncheckedIOException ex) {
                throw ex.getCause();
            }
        }

        public void writeList(List<?> list) throws IOException {
            writeInt(list.size());
            try {
                list.forEach(l -> {
                    try {
                        writeObjectFast(l);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            } catch (UncheckedIOException ex) {
                throw ex.getCause();
            }
        }

        public void writeObjectFast(Object o) throws IOException {
            if (o == null || o == NullOrMissingValue.NULL) {
                write(TYPE.NULL.ordinal());
            } else if (Boolean.TRUE.equals(o)) {
                write(TYPE.TRUE.ordinal());
            } else if (Boolean.FALSE.equals(o)) {
                write(TYPE.FALSE.ordinal());
            } else if (o instanceof Byte) {
                write(TYPE.BYTE.ordinal());
                writeByte((Byte) o);
            } else if (o instanceof Short) {
                write(TYPE.SHORT.ordinal());
                writeShort((Short) o);
            } else if (o instanceof Integer) {
                write(TYPE.INT.ordinal());
                writeInt((Integer) o);
            } else if (o instanceof Long) {
                write(TYPE.LONG.ordinal());
                writeLong((Long) o);
            } else if (o instanceof Float) {
                write(TYPE.FLOAT.ordinal());
                writeFloat((Float) o);
            } else if (o instanceof Double) {
                write(TYPE.DOUBLE.ordinal());
                writeDouble((Double) o);
            } else if (o instanceof Character) {
                write(TYPE.CHAR.ordinal());
                writeChar((Character) o);
            } else if (isImmutable(o.getClass())) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(o);
            } else if (o instanceof Date) {
                write(TYPE.DATE.ordinal());
                writeLong(((Date) o).getTime());
            } else if (faster.containsKey(o.getClass())) {
                write(TYPE.FASTER.ordinal());
                Class<? extends ObjectFaster<?>> clazz = faster.get(o.getClass());
                try {
                    ObjectFaster<?> of = clazz.getConstructor(o.getClass()).newInstance(o);
                    writeObject(of);
                } catch (InstantiationException | NoSuchMethodException | IllegalAccessException |
                         InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            } else if (o instanceof Map && MAP_MAPPING.containsKey(o.getClass())) {
                write(TYPE.MAP.ordinal());
                writeInt(MAP_MAPPING.get(o.getClass()).ordinal());
                writeMap((Map<?, ?>) o);
            } else if (o instanceof List) {
                write(TYPE.LIST.ordinal());
                writeList((List<?>) o);
            } else if (ConnectionContext.EMPTY.equals(o)) {
                write(TYPE.EMPTY_CONTEXT.ordinal());
            } else if (o.getClass().isArray()) {
                writeArray(o);
            } else {
                write(TYPE.OTHER.ordinal());
                writeObject(o);
            }
        }

        private void writeArray(Object o) throws IOException {
            Class<?> component = o.getClass().getComponentType();
            int length = Array.getLength(o);
            if (component.isPrimitive()) {
                write(TYPE.IMMUTABLE.ordinal());
                if (component == Boolean.TYPE) {
                    writeReference(Arrays.copyOf((boolean[]) o, length));
                } else if (component == Byte.TYPE) {
                    writeReference(Arrays.copyOf((byte[])  o, length));
                } else if (component == Short.TYPE) {
                    writeReference(Arrays.copyOf((short[])  o, length));
                } else if (component == Integer.TYPE) {
                    writeReference(Arrays.copyOf((int[])  o, length));
                } else if (component == Long.TYPE) {
                    writeReference(Arrays.copyOf((long[])  o, length));
                } else if (component == Float.TYPE) {
                    writeReference(Arrays.copyOf((float[])  o, length));
                } else if (component == Double.TYPE) {
                    writeReference(Arrays.copyOf((double[])  o, length));
                } else if (component == Character.TYPE) {
                    writeReference(Arrays.copyOf((char[])  o, length));
                }
            } else if (isImmutable(component)) {
                // Explicit handling because DomainSocketAddress is not yet know to Java 11
                // So it can not be explicitly added to the immutables Set
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(Arrays.copyOf((Object[]) o, length));
             } else {
                write(TYPE.ARRAY.ordinal());
                // A generic array, not trying to be smart, as there is no easy way to insert a null
                writeInt(length);
                writeReference(Array.newInstance(component, length));
                for (int i = 0; i < length; i++) {
                    writeObjectFast(Array.get(o, i));
                }
            }
        }

        @Override
        public void writeUTF(String str) throws IOException {
            writeReference(str);
        }

        private void writeReference(Object o) throws IOException {
            long currentRef = ref.getAndIncrement();
            writeLong(currentRef);
            immutableObjectsCache.put(currentRef, o);
        }

        @Override
        public void close() throws IOException {
            super.close();
            ref.set(0);
            immutableObjectsCache.clear();
        }

    }

    private static boolean isImmutable(Class<?> oClass) {
        return SocketAddress.class.isAssignableFrom(oClass)
               || InetAddress.class.isAssignableFrom(oClass)
               || oClass.isEnum()
               || immutables.contains(oClass)
               || oClass.isAnnotationPresent(Immutable.class);
    }

    public abstract static class ObjectFaster<T> implements Externalizable {
        protected T value;
        protected ObjectFaster(T o) {
            value = o;
        }
        protected ObjectFaster() {
            // No value
        }
        public T get() {
            return value;
        }
    }

    private static class EnumMapSerializer extends ObjectFaster<EnumMap<?, ?>> {
        public EnumMapSerializer(EnumMap o) {
            super(o);
        }

        public EnumMapSerializer() {
        }

        @Override
        public void writeExternal(ObjectOutput output) throws IOException {
            if (! (output instanceof FastObjectOutputStream)) {
                throw new IllegalStateException();
            } else {
                FastObjectOutputStream foos = (FastObjectOutputStream) output;
                EnumMap<?, ?> newMap = new EnumMap<>(value);
                newMap.clear();
                foos.writeReference(newMap);
                foos.writeInt(value.size());
                for(Object i: get().entrySet()) {
                    Map.Entry<?, ?>  e = (Map.Entry<?, ?>)i;
                    foos.writeObjectFast(e.getKey());
                    foos.writeObjectFast(e.getValue());
                }
            }
        }
        @Override
        public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
            if (! (input instanceof FastObjectInputStream)) {
                throw new IllegalStateException();
            } else {
                FastObjectInputStream fois = (FastObjectInputStream) input;
                Map<Object, Object> newMap = fois.readReference();
                for (int i = fois.readInt(); i > 0; i--) {
                    Object k = fois.readObjectFast();
                    Object v = fois.readObjectFast();
                    newMap.put(k, v);
                }
                value = (EnumMap) newMap;
            }
        }
    }

    private static final Map<Class<?>, Class<? extends ObjectFaster<?>>> faster = new HashMap<>();
    private static final Set<Class<?>> immutables = new HashSet<>();
    static {
        immutables.add(Instant.class);
        immutables.add(Period.class);
        immutables.add(Duration.class);
        immutables.add(HijrahDate.class);
        immutables.add(JapaneseDate.class);
        immutables.add(LocalDate.class);
        immutables.add(MinguoDate.class);
        immutables.add(ZonedDateTime.class);
        immutables.add(OffsetDateTime.class);
        immutables.add(OffsetTime.class);
        immutables.add(ThaiBuddhistDate.class);
        immutables.add(Year.class);
        immutables.add(YearMonth.class);
        immutables.add(LocalDateTime.class);
        immutables.add(LocalTime.class);
        immutables.add(Character.class);
        immutables.add(String.class);
        immutables.add(Boolean.class);
        immutables.add(Byte.class);
        immutables.add(Short.class);
        immutables.add(Integer.class);
        immutables.add(Long.class);
        immutables.add(Float.class);
        immutables.add(Double.class);

        faster.put(EnumMap.class, EnumMapSerializer.class);
    }

    public static <T> void register(Class<T> clazz, Class<? extends ObjectFaster<T>> of) {
        faster.put(clazz, of);
    }
    public static void registerImmutable(Class<?> clazz) {
        immutables.add(clazz);
    }

    private FastExternalizeObject() {
        // Not instantiable class
    }

}
