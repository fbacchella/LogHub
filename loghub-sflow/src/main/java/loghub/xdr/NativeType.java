package loghub.xdr;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public enum NativeType {

    BYTE_ARRAY(-1, NativeType::readBytes, "opaque") {
        @Override
        String getString(int size) {
            return String.format("opaque[%d]", size);
        }
    },
    STRING(-1, NativeType::readString, "string") {
        @Override
        String getString(int size) {
            return String.format("string[%d]", size);
        }
    },
    INT(4, ByteBuf::readInt, "int"),
    UNSIGNED_INT(4, ByteBuf::readUnsignedInt, "unsigned int"),
    HYPER(8, ByteBuf::readLong, "hyper"),
    UNSIGNED_HYPER(8, ByteBuf::readLong, "unsigned hyper"),
    FLOAT(4, ByteBuf::readFloat, "float"),
    DOUBLE(8, ByteBuf::readDouble, "double"),
    QUADRUPLE(16, b -> null, "quadruple"),
    BOOL(1, b -> b.readByte() == 1, "bool");

    public final ReadType<?> reader;
    public final int size;
    public final String typeName;

    NativeType(int size, ReadType<?> reader, String typeName) {
        this.size = size;
        this.reader = reader;
        this.typeName = typeName;
    }

    String getString(int size) {
        return typeName;
    }

    private static byte[] readBytes(ByteBuf buffer) {
        int length = Math.toIntExact(buffer.readUnsignedInt());
        byte[] data = new byte[length];
        buffer.readBytes(data);
        return data;
    }

    private static String readString(ByteBuf buffer) {
        int length = Math.toIntExact(buffer.readUnsignedInt());
        byte[] data = new byte[length];
        buffer.readBytes(data);
        if (length % 4 != 0) {
            buffer.skipBytes(length % 4);
        }
        return new String(data, StandardCharsets.US_ASCII);
    }


}
