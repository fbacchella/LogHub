package loghub.kafka;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Getter;

public enum HeadersTypes {
    UNKNOWN(0) {
        public Object read(byte[] content) {
            return content.clone();
        }
        public byte[] write(Object value) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
    },
    DOUBLE(1) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getDouble);
        }
        public byte[] write(Object value) {
            return toBytes(Double.BYTES, buf -> buf.putDouble((Double) value));
        }
    },
    FLOAT(2) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getFloat);
        }
        public byte[] write(Object value) {
            return toBytes(Float.BYTES, buf -> buf.putFloat((Float) value));
        }
    },
    LONG(3) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getLong);
        }
        public byte[] write(Object value) {
            return toBytes(Long.BYTES, buf -> buf.putLong((Long)value));
        }
    },
    INTEGER(4) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getInt);
        }
        public byte[] write(Object value) {
            return toBytes(Integer.BYTES, buf -> buf.putInt((Integer) value));
        }
    },
    SHORT(5) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getShort);
        }
        public byte[] write(Object value) {
            return toBytes(Integer.BYTES, buf -> buf.putShort((Short) value));
        }
    },
    BYTE(6) {
        public Object read(byte[] content) {
            return content[0];
        }
        public byte[] write(Object value) {
            return new byte[] { (Byte)value };
        }
    },
    BOOLEAN(7) {
        public Object read(byte[] content) {
            return content[0] != 0;
        }
        public byte[] write(Object value) {
            return new byte[] { (byte)((boolean)value ? 1 : 0) };
        }
    },
    CHARACTER(8) {
        public Object read(byte[] content) {
            return fromBytes(content, ByteBuffer::getChar);
        }
        public byte[] write(Object value) {
            return toBytes(Character.BYTES, buf -> buf.putChar((Character) value));
        }
    },
    STRING(9) {
        public Object read(byte[] content) {
            return new String(content, StandardCharsets.UTF_8);
        }
        public byte[] write(Object value) {
            return ((String)value).getBytes(StandardCharsets.UTF_8);
        }
    },
    BYTE_ARRAY(10) {
        public Object read(byte[] content) {
            return content.clone();
        }
        public byte[] write(Object value) {
            return ((byte[])value).clone();
        }
    },
    INET_ADDRESS(11) {
        public Object read(byte[] content) {
            try {
                return InetAddress.getByAddress(content);
            } catch (UnknownHostException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        public byte[] write(Object value) {
            return ((InetAddress) value).getAddress();
        }
    },
    NULL(12) {
        public Object read(byte[] content) {
            return null;
        }
        public byte[] write(Object value) {
            return null;
        }
    }
    ;

    private static byte[] toBytes(int size, Consumer<ByteBuffer> writer) {
        byte[] buffer = new byte[size];
        writer.accept(ByteBuffer.wrap(buffer).order(BYTE_ORDER));
        return buffer;
    }

    private static <T> T fromBytes(byte[] content, Function<ByteBuffer, T> reader) {
        return reader.apply(ByteBuffer.wrap(content).order(BYTE_ORDER));
    }

    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final HeadersTypes[] ID_CACHE = new HeadersTypes[HeadersTypes.values().length];
    static {
        for (HeadersTypes type: values()) {
            ID_CACHE[type.id & 0xFF] = type;
        }
    }
    public static final String KEYTYPE_HEADER_NAME = "LogHubKeyType";
    public static final String DATE_HEADER_NAME = "Date";
    public static final String CONTENTYPE_HEADER_NAME = "Content-Type";

    @Getter
    private final byte id;

    HeadersTypes(int id) {
        this.id = (byte)id;
    }

    public abstract Object read(byte[] content);
    public abstract byte[] write(Object value);

    @SuppressWarnings("unused")
    public static HeadersTypes resolve(Object value) {
        return switch (value) {
            case Long l -> LONG;
            case Double v -> DOUBLE;
            case Float v -> FLOAT;
            case Integer i -> INTEGER;
            case Short i -> SHORT;
            case Byte b -> BYTE;
            case Boolean b -> BOOLEAN;
            case Character c -> CHARACTER;
            case String s -> STRING;
            case byte[] bytes -> BYTE_ARRAY;
            case InetAddress inetAddress -> INET_ADDRESS;
            case null -> NULL;
            default -> UNKNOWN;
        };
    }

    public static HeadersTypes getById(int id) {
        if (id < 0 || id >= ID_CACHE.length) {
            throw new IllegalArgumentException("Unknown type ID: " + id);
        } else {
            return ID_CACHE[id & 0xFF];
        }
    }

}
