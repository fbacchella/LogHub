package loghub.jackson;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackGenerator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public abstract class MsgpackTimeSerializer<K> extends JsonSerializer<K> {

    public static class DateSerializer extends MsgpackTimeSerializer<Date> {
        @Override
        public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            long seconds = Math.floorDiv(value.getTime(), 1000L);
            int nanoseconds = ((int)(value.getTime() % 1000L)) * 1000000;
            doSerialiaze(seconds, nanoseconds, (MessagePackGenerator) gen);
        }

        @Override
        public Class<Date> handledType() {
            return Date.class;
        }
    }

    public static class InstantSerializer extends MsgpackTimeSerializer<Instant> {
        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            long seconds = value.getEpochSecond();
            int nanoseconds = value.getNano();
            doSerialiaze(seconds, nanoseconds, (MessagePackGenerator) gen);
        }

        @Override
        public Class<Instant> handledType() {
            return Instant.class;
        }
    }

    void doSerialiaze(long seconds, int nanoseconds, MessagePackGenerator gen) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.wrap(new byte[12]);
        longBuffer.order(ByteOrder.BIG_ENDIAN);
        long result = ((long)nanoseconds << 34) | seconds;
        int size = 0;
        if ((result >> 34) == 0) {
            if ((result & 0xffffffff00000000L) == 0 ) {
                longBuffer.putInt((int) result);
                size = 4;
            } else {
                longBuffer.putLong(result);
                size = 8;
            }
        } else {
            longBuffer.putInt(nanoseconds);
            longBuffer.putLong(seconds);
            size = 12;
        }
        MessagePackExtensionType ext = new MessagePackExtensionType((byte)-1, Arrays.copyOf(longBuffer.array(), size));
        gen.writeExtensionType(ext);
    }

}