package loghub;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        MAP,
        LIST,
        EMPTY_CONTEXT,
        FASTER,
        OTHER,
    }

    public static class FastObjectInputStream extends ObjectInputStream {

        @Getter
        private final EventsFactory factory;

        public FastObjectInputStream(byte[] buffer, EventsFactory factory) throws IOException {
            super(new ByteArrayInputStream(buffer));
            this.factory = factory;
        }

        private Map readMap() throws IOException, ClassNotFoundException {
            Map<Object, Object> map = new HashMap<>();
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
                return readUTF();
            case DATE:
                return new Date(readLong());
            case INSTANT:
                return Instant.ofEpochSecond(readLong(), readLong());
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

    }

    public static class FastObjectOutputStream extends ObjectOutputStream {

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
                writeUTF((String)o);
            } else if (o instanceof Date) {
                write(TYPE.DATE.ordinal());
                writeLong(((Date)o).getTime());
            } else if (o instanceof Instant) {
                write(TYPE.INSTANT.ordinal());
                Instant i = (Instant) o;
                writeLong(i.getEpochSecond());
                writeLong(i.getNano());
            } else if (o instanceof HashMap) {
                write(TYPE.MAP.ordinal());
                writeMap((Map<Object, Object>) o);
            } else if (o instanceof ArrayList || o instanceof LinkedList) {
                write(TYPE.LIST.ordinal());
                writeList((List<Object>) o);
            } else if (ConnectionContext.EMPTY.equals(o)) {
                write(TYPE.EMPTY_CONTEXT.ordinal());
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

    public static <T> void register(Class<T> clazz, Class<? extends ObjectFaster<T>> of) {
        faster.put(clazz, of);
    }
    private FastExternalizeObject() {
        // Not instantiable class
    }
}
