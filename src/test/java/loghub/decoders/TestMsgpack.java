package loghub.decoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.receivers.Receiver;

public class TestMsgpack {

    private static Logger logger;

    private final static ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final static Map<String, Object> obj = new HashMap<String, Object>();

    static {
        obj.put("a", "0");
        obj.put("b", 1);
        obj.put("c", false);
        obj.put("d", new Object[]{"0", 1, 2.0, null});
        obj.put("f", new Date(1000));
        obj.put("g", Instant.ofEpochSecond(2, 3000000));
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testmap() throws IOException, DecodeException {
        Msgpack d = new Msgpack.Builder().build();

        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, objectMapper.writeValueAsBytes(obj)).findAny().get();

        testContent(e);

    }

    @Test
    public void testmapsimple() throws IOException, DecodeException {
        Decoder d = new Msgpack.Builder().build();

        Map<Value, Value> destination = new HashMap<>();
        destination.put(ValueFactory.newString("a"), ValueFactory.newString("0"));
        destination.put(ValueFactory.newString("b"), ValueFactory.newInteger(1));
        destination.put(ValueFactory.newString("c"), ValueFactory.newBoolean(false));
        Value[] subdestination = new Value[4];
        subdestination[0] = ValueFactory.newString("0");
        subdestination[1] = ValueFactory.newInteger(1);
        subdestination[2] = ValueFactory.newFloat(2.0);
        subdestination[3] = ValueFactory.newNil();
        destination.put(ValueFactory.newString("d"), ValueFactory.newArray(subdestination));

        Value v = ValueFactory.newMap(destination);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessagePacker packer = MessagePack.newDefaultPacker(out);

        packer.packValue(v);
        packer.close();
        byte[] packed = out.toByteArray();

        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, packed).findAny().get();
        testContent(e);
    }
    @Test
    public void testtimestamps() throws IOException, DecodeException {
        Decoder d = new Msgpack.Builder().build();

        Map<Value, Value> destination = new HashMap<>();
        destination.put(ValueFactory.newString("a"), ValueFactory.newExtension((byte) -1, new byte[]{1, 2, 3, 4}) );
        destination.put(ValueFactory.newString("b"), ValueFactory.newExtension((byte) -1, new byte[]{1, 2, 3, 4, 5, 6, 7, 8}) );
        destination.put(ValueFactory.newString("c"), ValueFactory.newExtension((byte) -1, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}) );
        destination.put(ValueFactory.newString("d"), ValueFactory.newExtension((byte) -1, new byte[]{12, 11, 10, 9, 0, 0, 0, 0, 0, 3, 2, 1}) );

        // A value that overflow the unsigned nano
        ByteBuffer bytes = ByteBuffer.wrap(new byte[8]);
        bytes.order(ByteOrder.BIG_ENDIAN);
        bytes.putLong(Long.parseUnsignedLong("12005275407473284400"));
        destination.put(ValueFactory.newString("e"), ValueFactory.newExtension((byte) -1, bytes.array()) );

        Value v = ValueFactory.newMap(destination);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessagePacker packer = MessagePack.newDefaultPacker(out);

        packer.packValue(v);
        packer.close();
        byte[] packed = out.toByteArray();

        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, packed).findAny().get();
        Assert.assertEquals(Instant.class, e.get("a").getClass());
        Assert.assertEquals(Instant.class, e.get("b").getClass());
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, (byte[]) e.get("c"));
        Assert.assertEquals(Instant.class, e.get("d").getClass());
        Instant time = (Instant)e.get("e");
        Assert.assertEquals(1563268400L, time.getEpochSecond());
        Assert.assertEquals(698799000, time.getNano());
    }

    @Test
    public void testDecoder() throws JsonProcessingException {

        byte[] bs = objectMapper.writeValueAsBytes(obj);

        try(Receiver r = new Receiver() {
            @Override
            public String getReceiverName() {
                return null;
            }

            @Override
            public Event next() {
                return decodeStream(ConnectionContext.EMPTY, bs).findAny().get();
            }

            @Override
            public void run() {
            }
        }) {
            r.setTimeStampField("f");
            Msgpack d = AbstractBuilder.resolve(Msgpack.class).build();
            r.setDecoder(d);
            Event e = r.next();
            testContent(e);
        }
    }

    @Test
    public void testRoundTripAsMap() throws IOException, DecodeException {
        loghub.encoders.Msgpack.Builder builder = loghub.encoders.Msgpack.getBuilder();
        builder.setForwardEvent(false);
        loghub.encoders.Msgpack enc = builder.build();
        Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
        ev.putAll(obj);
        ev.putMeta("h", 7);
        ev.setTimestamp(new Date(0));
        Decoder dec = new Msgpack.Builder().build();
        Map<String, Object> e = dec.decode(ConnectionContext.EMPTY, enc.encode(ev)).findAny().get();
        testContent(e);
        Assert.assertFalse(e instanceof Event);
        Instant f = (Instant) e.get("f");
        Instant g = (Instant) e.get("g");
        Assert.assertEquals(1, f.getEpochSecond());
        Assert.assertEquals(2, g.getEpochSecond());
        Assert.assertEquals(3000000, g.getNano());
    }

    private void testContent(Map<String, Object> e) {
        Assert.assertEquals("key a not found", "0", e.get("a"));
        Assert.assertEquals("key b not found", 1, e.get("b"));
        Assert.assertEquals("key c not found", false, e.get("c"));
        @SuppressWarnings("unchecked")
        List<Object> l = (List<Object>) e.get("d");
        Assert.assertEquals("array element 0 not found", "0", l.get(0));
        Assert.assertEquals("array element 1 not found", 1, l.get(1));
        Assert.assertEquals("array element 2 not found", 2.0, l.get(2));
        Assert.assertEquals("array element 3 not found", null, l.get(3));
    }

}
