package loghub.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import lombok.Getter;

public enum KeyTypes {
    DOUBLE(0) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getDouble();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[8]; // Correction: double fait 8 bytes
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putDouble((Double)value);
            return buffer;
        }
    },
    FLOAT(1) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getFloat();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[4];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putFloat((Float)value);
            return buffer;
        }
    },
    LONG(2) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getLong();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[8];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putLong((Long)value);
            return buffer;
        }
    },
    INTEGER(3) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getInt();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[4];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putInt((Integer)value);
            return buffer;
        }
    },
    SHORT(4) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getShort();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[2];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putShort((Short)value);
            return buffer;
        }
    },
    BYTE(5) {
        public Object read(byte[] content) {
            return content[0];
        }
        public byte[] write(Object value) {
            return new byte[] { (Byte)value };
        }
    },
    BOOLEAN(6) {
        public Object read(byte[] content) {
            return content[0] != 0;
        }
        public byte[] write(Object value) {
            return new byte[] { (byte)((boolean)value ? 1 : 0) };
        }
    },
    CHARACTER(7) {
        public Object read(byte[] content) {
            return ByteBuffer.wrap(content).order(BYTE_ORDER).getChar();
        }
        public byte[] write(Object value) {
            byte[] buffer = new byte[2];
            ByteBuffer.wrap(buffer).order(BYTE_ORDER).putChar((Character)value);
            return buffer;
        }
    },
    STRING(8) {
        public Object read(byte[] content) {
            return new String(content, StandardCharsets.UTF_8);
        }
        public byte[] write(Object value) {
            return ((String)value).getBytes(StandardCharsets.UTF_8);
        }
    },
    BYTE_ARRAY(9) {
        public Object read(byte[] content) {
            return content.clone();
        }
        public byte[] write(Object value) {
            return ((byte[])value).clone();
        }
    };

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

    public static KeyTypes resolve(Object value) {
        if (value instanceof Long) {
            return LONG;
        } else if (value instanceof Double) {
            return DOUBLE;
        } else if (value instanceof Float) {
            return FLOAT;
        } else if (value instanceof Integer) {
            return INTEGER;
        } else if (value instanceof Short) {
            return SHORT;
        } else if (value instanceof Byte) {
            return BYTE;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof Character) {
            return CHARACTER;
        } else if (value instanceof String) {
            return STRING;
        } else if (value instanceof byte[]) {
            return BYTE_ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
        }
    }

    public static KeyTypes getById(int id) {
        if (id > ID_CACHE.length || id < 0) {
            throw new IllegalArgumentException("Unknown type ID: " + id);
        } else {
            return ID_CACHE[id & 0xFF];
        }
    }

}
