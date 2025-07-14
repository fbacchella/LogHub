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

import loghub.NullOrMissingValue;

import static loghub.cloners.MapCloner.MAP_MAPPING;

public class CloneOpaque {

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

    public static class FastObjectInputStream extends ObjectInputStream {
        private final FastObjectOutputStream out;

        public FastObjectInputStream(byte[] buffer, FastObjectOutputStream out) throws IOException {
            super(new ByteArrayInputStream(buffer));
            this.out = out;
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
            case IMMUTABLE:
                return readReference();
            case FASTER:
                ObjectFaster<?> of = (ObjectFaster<?>) readObject();
                return of.get();
            default:
                return readObject();
            }
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

    public static class FastObjectOutputStream extends ObjectOutputStream {

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
            } else if (DeepCloner.isImmutable(o)) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(o);
            } else if (o instanceof Map && MAP_MAPPING.containsKey(o.getClass())) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(MapCloner.clone((Map<?, ?>) o));
            } else if (o instanceof List) {
                write(TYPE.IMMUTABLE.ordinal());
                writeReference(CloneList.clone((List<Object>) o));
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

    public static <T> T clone(T object) {
        // FastObjectInputStream
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); FastObjectOutputStream oos = new FastObjectOutputStream(bos)) {
            oos.writeObjectFast(object);
            oos.flush();
            bos.flush();
            try (FastObjectInputStream ois = new FastObjectInputStream(bos.toByteArray(), oos)) {
                return ((T) ois.readObjectFast());
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

    }

    private CloneOpaque() {
        // Not instantiable class
    }

}
