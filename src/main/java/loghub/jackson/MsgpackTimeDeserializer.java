package loghub.jackson;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;

import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;

public class MsgpackTimeDeserializer implements ExtensionTypeCustomDeserializers.Deser {

    @Override
    public Object deserialize(byte[] data) throws IOException {
        ByteBuffer content = ByteBuffer.wrap(data);
        long seconds = 0;
        int nanoseconds = 0;
        switch (data.length) {
        case 4:
            seconds = content.getInt();
            nanoseconds = 0;
            break;
        case 8:
            long lcontent = content.getLong();
            seconds = lcontent & 0x00000003ffffffffL;
            // Masked needed to drop sign extended by right shift
            nanoseconds = (int)((lcontent >> 34) & (0x3FFFFFFFL));
            break;
        case 12:
            nanoseconds = content.getInt();
            seconds = content.getLong();
            break;
        default:
            throw new IOException("Invalid time object length");
        }
        try {
            return Instant.ofEpochSecond(seconds, nanoseconds);
        } catch (DateTimeException e) {
            return data;
        }
    }

}
