package loghub.decoders;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Decoder;
import loghub.Event;
import loghub.LogUtils;
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
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testmap() throws IOException {
        Decoder d = new Msgpack();

        Event e = new Event();
        d.decode(e, objectMapper.writeValueAsBytes(obj));

        testContent(e);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testother() throws IOException {

        byte[] bs = objectMapper.writeValueAsBytes(new Object[] {obj});

        Msgpack d = new Msgpack();
        d.setField("vector");
        Event e = new Event();
        d.decode(e, bs);

        List<Map<String, Object>> v = (List<Map<String, Object>>) e.get("vector");
        testContent(v.get(0));
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
