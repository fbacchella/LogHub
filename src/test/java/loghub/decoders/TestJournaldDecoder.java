package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;

public class TestJournaldDecoder {

    private static Logger logger;


    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.decoders.JournaldExport");
    }

    @Test
    public void testReadOnce() throws DecodeException, IOException {
        ByteBuf readBuffer = ByteBufAllocator.DEFAULT.buffer(4096);
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("binaryjournald")) {
            int read = 0;
            do {
                read = readBuffer.writeBytes(is, 4096);
            } while (read > 0);
        }
        JournaldExport decoder = JournaldExport.getBuilder().build();
        List<Map<String, Object>> events = decoder.decode(ConnectionContext.EMPTY, readBuffer).collect(Collectors.toList());
        check(events);
    }

    @Test
    public void testReadSplitted() throws DecodeException, IOException {
        CompositeByteBuf chunksBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();
        JournaldExport decoder = JournaldExport.getBuilder().build();
        List<Map<String, Object>> events = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("binaryjournald")) {
            int read = 0;
            do {
                ByteBuf readBuffer = ByteBufAllocator.DEFAULT.buffer(1024);
                read = readBuffer.writeBytes(is, 1024);
                chunksBuffer.addComponent(true, readBuffer);
                decoder.decode(ConnectionContext.EMPTY, chunksBuffer).forEach(events::add);
            } while (read > 0);
        }
        check(events);
    }

    @SuppressWarnings("unchecked")
    private void check(List<Map<String, Object>> events) {
        Assert.assertEquals(4, events.size());
        events.forEach(e -> Assert.assertTrue(e instanceof Event));
        Event ev = (Event) events.get(0);
        Assert.assertEquals(1637065000943L, ev.getTimestamp().getTime());
        String message = (String) ((Map<String, Object>)ev.get("fields_user")).get("message");
        Assert.assertEquals("Upload to http://xxxxxxxxxxxxxxxxxxxxxxxxxxxxx/upload failed: Send failure: Broken pipe", message);
        String uid = (String) ((Map<String, Object>)ev.get("fields_trusted")).get("uid");
        Assert.assertEquals("461", uid);

        // Checks the last event
        ev = (Event) events.get(3);
        Assert.assertEquals(1637065006095L, ev.getTimestamp().getTime());
        events.forEach(e -> Assert.assertFalse(((Map<String, Object>)e.get("fields_trusted")).containsKey("realtime_timestamp")));
        events.forEach(e -> Assert.assertFalse(((Map<String, Object>)e.get("fields_trusted")).containsKey("source_realtime_timestamp")));
        
        events.stream().map(e -> (Event) e)
                       .map(Event::getTimestamp)
                       .map(Date::getTime)
                       .forEach(i -> Assert.assertTrue(i <= 1637065006095L));
    }

    @Test
    public void test_loghub_decoders_JournaldExport() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.JournaldExport");
    }

}
