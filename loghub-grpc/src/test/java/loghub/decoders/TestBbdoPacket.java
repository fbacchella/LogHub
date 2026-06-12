package loghub.decoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import loghub.grpc.BinaryCodec;

class TestBbdoPacket {

    private static BinaryCodec codec;

    @BeforeAll
    static void setup() {
        codec = CentreonBroker.getBuilder().build().getProtobufCodec();
    }

    @Test
    void serializeAndDeserializeKnownEvent() throws IOException {
        Map<String, Object> payload = Map.of("broker_name", "test-broker", "peer_type", "BROKER");
        BbdoPacket original = new BbdoPacket(BbdoEvent.BBDO_WELCOME, 1, 2, payload);

        ByteBuffer serialized = original.serialize(codec);
        BbdoPacket deserialized = BbdoPacket.of(codec, serialized);

        Assertions.assertEquals(BbdoEvent.BBDO_WELCOME, deserialized.event());
        Assertions.assertEquals(1, deserialized.sourceId());
        Assertions.assertEquals(2, deserialized.destinationId());
        Assertions.assertEquals("test-broker", deserialized.payload().get("broker_name"));
        Assertions.assertEquals("BROKER", deserialized.payload().get("peer_type"));
    }

    @Test
    void serializeAndDeserializeKnownEventWithDefault() throws IOException {
        BbdoPacket original = new BbdoPacket(BbdoEvent.BBDO_WELCOME, 1, 2, Map.of());

        ByteBuffer serialized = original.serialize(codec);
        BbdoPacket deserialized = BbdoPacket.of(codec, serialized);

        Assertions.assertEquals(BbdoEvent.BBDO_WELCOME, deserialized.event());
        Assertions.assertEquals(1, deserialized.sourceId());
        Assertions.assertEquals(2, deserialized.destinationId());
        Assertions.assertEquals("", deserialized.payload().get("broker_name"));
        Assertions.assertEquals("UNKNOWN", deserialized.payload().get("peer_type"));
    }

    @Test
    void serializeAndDeserializeNullEvent() throws IOException {
        byte[] rawData = new byte[]{0x01, 0x02, 0x03};
        Map<String, Object> payload = Map.of("data", rawData);
        BbdoPacket original = new BbdoPacket(null, 10, 20, payload);

        ByteBuffer serialized = original.serialize(codec);
        BbdoPacket deserialized = BbdoPacket.of(codec, serialized);

        Assertions.assertNull(deserialized.event());
        Assertions.assertEquals(10, deserialized.sourceId());
        Assertions.assertEquals(20, deserialized.destinationId());
        Assertions.assertArrayEquals(rawData, (byte[]) deserialized.payload().get("data"));
    }

    @Test
    void fromIdReturnsCorrectEvent() {
        int id = (BbdoCategory.NEB.value << 16) | 35;
        BbdoEvent event = BbdoEvent.fromId(id);
        Assertions.assertEquals(BbdoEvent.NEB_PB_COMMENT, event);
    }

    @Test
    void fromIdReturnsNullForUnknownId() {
        BbdoEvent event = BbdoEvent.fromId(0xDEAD);
        Assertions.assertNull(event);
    }

    @Test
    void invalidChecksumThrowsException() {
        Map<String, Object> payload = Map.of("broker_name", "test", "peer_type", "BROKER");
        BbdoPacket original = new BbdoPacket(BbdoEvent.BBDO_WELCOME, 1, 2, payload);
        ByteBuffer serialized = original.serialize(codec);

        byte[] bytes = new byte[serialized.remaining()];
        serialized.get(bytes);
        bytes[0] = (byte) ~bytes[0];
        bytes[1] = (byte) ~bytes[1];
        ByteBuffer corrupted = ByteBuffer.wrap(bytes);

        Assertions.assertThrows(IOException.class, () -> BbdoPacket.of(codec, corrupted));
    }

    @Test
    void invalidSizeThrowsException() {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putShort((short) 0);
        bb.putShort((short) 999);
        bb.putInt(0);
        bb.putInt(0);
        bb.putInt(0);
        bb.flip();

        Assertions.assertThrows(IOException.class, () -> BbdoPacket.of(codec, bb));
    }

    @Test
    void bbdoEventToStringContainsCategoryAndMessage() {
        String str = BbdoEvent.BBDO_WELCOME.toString();
        Assertions.assertTrue(str.contains("BBDO_WELCOME"), "toString should contain the event name");
        Assertions.assertTrue(str.contains("BBDO"), "toString should contain the category name");
        Assertions.assertTrue(str.contains("7"), "toString should contain the message number");
    }

    @Test
    void bbdoCategoryFromValue() {
        Assertions.assertEquals(BbdoCategory.NEB, BbdoCategory.fromValue(1));
        Assertions.assertEquals(BbdoCategory.BBDO, BbdoCategory.fromValue(2));
        Assertions.assertEquals(BbdoCategory.STORAGE, BbdoCategory.fromValue(3));
        Assertions.assertEquals(BbdoCategory.BAM, BbdoCategory.fromValue(6));
        Assertions.assertNull(BbdoCategory.fromValue(999));
    }

    @Test
    void bbdoEventCategoryIsCorrect() {
        Assertions.assertEquals(BbdoCategory.NEB, BbdoEvent.NEB_PB_SERVICE.category);
        Assertions.assertEquals(BbdoCategory.BBDO, BbdoEvent.BBDO_PB_ACK.category);
        Assertions.assertEquals(BbdoCategory.STORAGE, BbdoEvent.STORAGE_PB_METRIC.category);
        Assertions.assertEquals(BbdoCategory.BAM, BbdoEvent.BAM_PB_BA_STATUS.category);
    }
}
