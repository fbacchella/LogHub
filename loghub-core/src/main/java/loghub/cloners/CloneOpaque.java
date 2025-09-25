package loghub.cloners;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.NullOrMissingValue;

import static loghub.cloners.MapCloner.MAP_MAPPING;

class CloneOpaque {

    private static final Logger logger = LogManager.getLogger();

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
        IMMUTABLE,
        FASTER,
        OTHER,
    }
    private static final TYPE[] TYPES = TYPE.values();

    static class FastObjectInputStream extends ObjectInputStream {
        private final FastObjectOutputStream out;

        public FastObjectInputStream(byte[] buffer, FastObjectOutputStream out) throws IOException {
            super(new ByteArrayInputStream(buffer));
            this.out = out;
        }

        public Object readObjectFast() throws IOException, ClassNotFoundException {
            TYPE type = TYPES[read()];
            return switch (type) {
                case NULL -> NullOrMissingValue.NULL;
                case TRUE -> true;
                case FALSE -> false;
                case BYTE -> readByte();
                case SHORT -> readShort();
                case INT -> readInt();
                case LONG -> readLong();
                case FLOAT -> readFloat();
                case DOUBLE -> readDouble();
                case CHAR -> readChar();
                case IMMUTABLE -> readReference();
                case FASTER -> {
                    ObjectFaster<?> of = (ObjectFaster<?>) readObject();
                    yield of.get();
                }
                default -> readObject();
            };
        }

        @SuppressWarnings("unchecked")
        <T> T readReference() throws IOException {
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

    @SuppressWarnings("unchecked")
    static class FastObjectOutputStream extends ObjectOutputStream {
        // Immutable objects can be reused, just forward a reference
        private final Map<Long, Object> immutableObjectsCache = new HashMap<>();
        private final AtomicLong ref = new AtomicLong(0);

        public FastObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        public void writeObjectFast(Object o) throws IOException {
            if (o == null) {
                write(TYPE.NULL.ordinal());
            } else if (Boolean.TRUE.equals(o)) {
                write(TYPE.TRUE.ordinal());
            } else if (Boolean.FALSE.equals(o)) {
                write(TYPE.FALSE.ordinal());
            } else if (o instanceof Byte b) {
                write(TYPE.BYTE.ordinal());
                writeByte(b);
            } else if (o instanceof Short s) {
                write(TYPE.SHORT.ordinal());
                writeShort(s);
            } else if (o instanceof Integer i) {
                write(TYPE.INT.ordinal());
                writeInt(i);
            } else if (o instanceof Long l) {
                write(TYPE.LONG.ordinal());
                writeLong(l);
            } else if (o instanceof Float f) {
                write(TYPE.FLOAT.ordinal());
                writeFloat(f);
            } else if (o instanceof Double d) {
                write(TYPE.DOUBLE.ordinal());
                writeDouble(d);
            } else if (o instanceof Character c) {
                write(TYPE.CHAR.ordinal());
                writeChar(c);
            } else if (DeepCloner.isImmutable(o)) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(o);
            } else if (o instanceof Map && MAP_MAPPING.containsKey(o.getClass())) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(MapCloner.clone((Map<?, ?>) o));
            } else if (o instanceof List) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(CloneCollection.clone((List<Object>) o));
            } else if (o.getClass().isArray()) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(CloneArray.clone(o));
            } else if (o instanceof Cloneable) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(DeepCloner.tryClone(o));
            } else {
                write(TYPE.OTHER.ordinal());
                writeObject(o);
            }
        }

        @Override
        public void writeUTF(String str) throws IOException {
            writeReference(str);
        }

        void writeReference(Object o) throws IOException {
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

    @SuppressWarnings("unchecked")
    static <T> T clone(T object) {
        logger.warn("Failback to byte serialization clone for {}", () -> object.getClass().getName());
        // FastObjectInputStream
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); FastObjectOutputStream oos = new FastObjectOutputStream(bos)) {
            oos.writeObjectFast(object);
            oos.flush();
            bos.flush();
            try (FastObjectInputStream ois = new FastObjectInputStream(bos.toByteArray(), oos)) {
                return ((T) ois.readObjectFast());
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new NotClonableException(object.getClass(), e);
        }
    }

    private CloneOpaque() {
        // Not instantiable class
    }

}
