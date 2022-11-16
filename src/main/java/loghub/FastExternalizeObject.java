package loghub;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import loghub.events.EventsFactory;
import lombok.Getter;

public class FastExternalizeObject {

    private enum TYPE {
        NULL,
        TRUE,
        FALSE,
        BYTE,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        CHAR,
        STRING,
        DATE,
        INSTANT,
        LINKEDMAP,
        HASHMAP,
        LIST,
        EMPTY_CONTEXT,
        INETSOCKETADDRESS,
        INEDADDRESS,
        IMMUTABLE,
        FASTER,
        OTHER,
    }

    public static class FastObjectInputStream extends ObjectInputStream {

        @Getter
        private final EventsFactory factory;
        private final FastObjectOutputStream out;

        public FastObjectInputStream(byte[] buffer, EventsFactory factory, FastObjectOutputStream out) throws IOException {
            super(new ByteArrayInputStream(buffer));
            this.factory = factory;
            this.out = out;
        }

        private Map readMap() throws IOException, ClassNotFoundException {
            Map<Object, Object> map = new HashMap<>();
            return readMap(map);
        }

        private Map readLinkedMap() throws IOException, ClassNotFoundException {
            Map<Object, Object> map = new LinkedHashMap<>();
            return readMap(map);
        }

        public Map readMap(Map map) throws IOException, ClassNotFoundException {
            int size = readInt();
            for (int i = 0; i < size; i++) {
                Object key = readObjectFast();
                Object value = readObjectFast();
                map.put(key, value);
            }
            return map;
        }

        public List readList() throws IOException, ClassNotFoundException {
            int size = readInt();
            List list = new ArrayList(size);
            for (int i = 0; i < size; i++) {
                list.add(readObjectFast());
            }
            return list;
        }

        public Object readObjectFast() throws IOException, ClassNotFoundException {
            TYPE type = TYPE.values()[read()];
            switch (type) {
            case NULL:
                return null;
            case TRUE:
                return true;
            case FALSE:
                return false;
            case BYTE:
                return readByte();
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
            case STRING:
            case INEDADDRESS:
            case INETSOCKETADDRESS:
            case INSTANT:
            case IMMUTABLE:
                return readReference();
            case DATE:
                return new Date(readLong());
            case LINKEDMAP:
                return readLinkedMap();
            case HASHMAP:
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

        private <T> T readReference() throws IOException {
            long ref = readLong();
            return (T) out.immutableObjectsCache.remove(ref);
        }

        @Override
        public String readUTF() throws IOException {
            return readReference();
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
            if (o == null) {
                write(TYPE.NULL.ordinal());
            } else if (Boolean.TRUE.equals(o)) {
                write(TYPE.TRUE.ordinal());
            } else if (Boolean.FALSE.equals(o)) {
                write(TYPE.FALSE.ordinal());
            } else if (o instanceof Byte) {
                write(TYPE.BYTE.ordinal());
                writeByte((Byte) o);
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
            } else if (o instanceof String) {
                write(TYPE.STRING.ordinal());
                writeReference(o);
            } else if (o instanceof InetSocketAddress) {
                write(TYPE.INETSOCKETADDRESS.ordinal());
                writeReference(o);
            } else if (o instanceof InetAddress) {
                write(TYPE.INEDADDRESS.ordinal());
                writeReference(o);
            } else if (o instanceof Date) {
                write(TYPE.DATE.ordinal());
                writeLong(((Date)o).getTime());
            } else if (o instanceof Instant) {
                write(TYPE.INSTANT.ordinal());
                writeReference(o);
            } else if (o instanceof LinkedHashMap) {
                write(TYPE.LINKEDMAP.ordinal());
                writeMap((Map<Object, Object>) o);
            } else if (o instanceof HashMap) {
                write(TYPE.HASHMAP.ordinal());
                writeMap((Map<Object, Object>) o);
            } else if (o instanceof ArrayList || o instanceof LinkedList) {
                write(TYPE.LIST.ordinal());
                writeList((List<Object>) o);
            } else if (ConnectionContext.EMPTY.equals(o)) {
                write(TYPE.EMPTY_CONTEXT.ordinal());
            } else if (immutable.contains(o.getClass())) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(o);
            } else if (faster.containsKey(o.getClass())) {
                write(TYPE.FASTER.ordinal());
                Class<? extends ObjectFaster<?>> clazz = faster.get(o.getClass());
                try {
                    ObjectFaster of = clazz.getConstructor(o.getClass()).newInstance(o);
                    writeObject(of);
                } catch (InstantiationException | NoSuchMethodException | IllegalAccessException |
                         InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                write(TYPE.OTHER.ordinal());
                writeObject(o);
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

    private static final Map<Class<?>, Class<? extends ObjectFaster<?>>> faster = new HashMap<>();
    private static final Set<Class<?>> immutable = new HashSet<>();

    public static <T> void register(Class<T> clazz, Class<? extends ObjectFaster<T>> of) {
        faster.put(clazz, of);
    }
    public static void registerImmutable(Class<?> clazz) {
        immutable.add(clazz);
    }
    private FastExternalizeObject() {
        // Not instantiable class
    }
}
