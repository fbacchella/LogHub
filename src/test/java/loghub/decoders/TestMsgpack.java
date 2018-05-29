package loghub.decoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.Event;
import loghub.Decoder.DecodeException;
import loghub.LogUtils;
import loghub.Receiver;
import loghub.Tools;

public class TestMsgpack {

    private static Logger logger;

    private final static ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final static Map<String, Object> obj = new HashMap<String, Object>();

    static {
        obj.put("a", "0");
        obj.put("b", 1);
        obj.put("c", false);
        obj.put("d", new Object[]{"0", 1, 2.0, null});
        obj.put(Event.TIMESTAMPKEY, "1970-01-01T00:00:00.123456+0200");
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testmap() throws IOException, DecodeException {
        Decoder d = new Msgpack();

        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, objectMapper.writeValueAsBytes(obj));

        testContent(e);
    }

    @Test
    public void testmapsimple() throws IOException, DecodeException {
        Decoder d = new Msgpack();

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

        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, packed);

        testContent(e);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testother() throws IOException, DecodeException {

        byte[] bs = objectMapper.writeValueAsBytes(new Object[] {obj});

        Msgpack d = new Msgpack();
        d.setField("vector");
        Map<String, Object> e = d.decode(ConnectionContext.EMPTY, bs);

        List<Map<String, Object>> v = (List<Map<String, Object>>) e.get("vector");
        testContent(v.get(0));
    }

    @Test
    public void testDecoder() throws JsonProcessingException {

        byte[] bs = objectMapper.writeValueAsBytes(obj);

        try(Receiver r = new Receiver(null, null) {
            @Override
            public String getReceiverName() {
                return null;
            }

            @Override
            public Event next() {
                return decode(ConnectionContext.EMPTY, bs);
            }
        }) {
            Msgpack d = new Msgpack();
            r.setDecoder(d);
            Event e = r.next();
            testContent(e);
            Assert.assertEquals(100, e.getTimestamp().getTime());
            System.out.println(e);
        }
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
