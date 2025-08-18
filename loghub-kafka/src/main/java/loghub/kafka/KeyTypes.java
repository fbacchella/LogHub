package loghub.kafka;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import lombok.Getter;

public enum KeyTypes {
    UNKNOWN(0) {
        public Object read(byte[] content) {
            return new String(content, StandardCharsets.UTF_8);
        }
        public byte[] write(Object value) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
    },
    DOUBLE(1) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getDouble();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[8];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putDouble((Double)value);
            return buffer;
        }
    },
    FLOAT(2) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getFloat();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[4];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putFloat((Float)value);
            return buffer;
        }
    },
    LONG(3) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getLong();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[8];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putLong((Long)value);
            return buffer;
        }
    },
    INTEGER(4) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getInt();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[4];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putInt((Integer)value);
            return buffer;
        }
    },
    SHORT(5) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getShort();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[2];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putShort((Short)value);
            return buffer;
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
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getChar();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[2];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putChar((Character)value);
            return buffer;
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

    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final KeyTypes[] ID_CACHE = new KeyTypes[KeyTypes.values().length];
    static {
        for (KeyTypes type : values()) {
            ID_CACHE[type.id & 0xFF] = type;
        }
    }
    public static final String HEADER_NAME = "LogHubKeyType";

    @Getter
    private final byte id;

    KeyTypes(int id) {
        this.id = (byte)id;
    }

    public abstract Object read(byte[] content);
    public abstract byte[] write(Object value);

    @SuppressWarnings("unused")
    public static KeyTypes resolve(Object value) {
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

    public static KeyTypes getById(int id) {
        if (id > ID_CACHE.length || id < 0) {
            throw new IllegalArgumentException("Unknown type ID: " + id);
        } else {
            return ID_CACHE[id & 0xFF];
        }
    }

}
