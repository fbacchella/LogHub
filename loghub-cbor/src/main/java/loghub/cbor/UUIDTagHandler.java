package loghub.cbor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class UUIDTagHandler extends CborTagHandler<UUID> {

    public UUIDTagHandler() {
        super(37, UUID.class);
    }

    @Override
    public UUID parse(CBORParser p) throws IOException {
        if (p.currentToken() != JsonToken.VALUE_EMBEDDED_OBJECT) {
            throw new IllegalStateException("Expected embedded binary object for UUID");
        }
        byte[] data = p.getBinaryValue();
        if (data.length != 16) {
            throw new IOException("UUID binary representation must be exactly 16 bytes");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    @Override
    public void write(UUID uuid, CBORGenerator p) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        p.writeBinary(buffer.array());
    }

}
