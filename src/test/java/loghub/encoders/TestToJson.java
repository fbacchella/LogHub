package loghub.encoders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import loghub.senders.InMemorySender;

public class TestToJson {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    private void check(boolean pretty, boolean textdate) throws EncodeException, JsonProcessingException {
        ToJson.Builder builder = ToJson.getBuilder();
        builder.setPretty(pretty);
        builder.setDateAsText(textdate);
        ToJson encoder = builder.build();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), InMemorySender.getBuilder().build()));
        Event e = factory.newEvent();
        e.put("K1", "V1");
        e.put("K2", 2);
        e.put("K3", true);
        e.put("K4", Instant.EPOCH);
        e.put("K5", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")));
        e.put("K6", new Date(0));

        byte[] result = encoder.encode(e);

        String formatted = new String(result, StandardCharsets.UTF_8);
        Assert.assertEquals(pretty, formatted.contains("\n"));
        ObjectReader reader = JacksonBuilder.get(JsonMapper.class).getReader();
        Map<String, Object> m = reader.readValue(formatted);
        Assert.assertEquals("V1", m.get("K1"));
        Assert.assertEquals(2, m.get("K2"));
        Assert.assertEquals(true, m.get("K3"));
        if (pretty || textdate) {
            Assert.assertEquals("1970-01-01T00:00:00Z", m.get("K4"));
            Assert.assertEquals("1970-01-01T00:00:00Z", m.get("K5"));
            Assert.assertEquals("1970-01-01T00:00:00.000+00:00", m.get("K6"));
        } else {
            Assert.assertEquals(0.0, (double) m.get("K4"), 1e-5);
            Assert.assertEquals(0.0, (double) m.get("K5"), 1e-5);
            Assert.assertEquals(0, m.get("K6"));
        }
    }

    @Test
    public void testPretty() throws JsonProcessingException, EncodeException {
        check(true, false);
    }

    @Test
    public void testCompact() throws JsonProcessingException, EncodeException {
        check(false, false);
    }

    @Test
    public void testDateText() throws JsonProcessingException, EncodeException {
        check(false, true);
    }

}
