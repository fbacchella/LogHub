package loghub.jackson;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.NullOrMissingValue;
import loghub.VariablePath;
import loghub.datetime.DatetimeProcessor;
import loghub.events.Event;
import loghub.events.EventsFactory;

import static loghub.jackson.JacksonBuilder.OBJECTREF;

public class TestEventSerializer {

    private final EventsFactory factory = new EventsFactory();

    @Test
    public void testSimple() throws JsonProcessingException {
        SimpleModule module = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "EventToJson"));
        module.addSerializer(new EventSerializer());

        ObjectWriter writer = JacksonBuilder.get(JsonMapper.class)
                                            .module(module)
                                            .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                            .getWriter();
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(1));
        ev.putMeta("ma", 1);
        Map<String, Object> d = new HashMap<>();
        d.put("a", NullOrMissingValue.NULL);
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        ev.putAtPath(VariablePath.of("a", "c"), null);
        ev.putAtPath(VariablePath.of("d"), d);
        ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(ZoneId.of("Z"));
        ev.putAtPath(VariablePath.of("b"), now);
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) ev.get("a");
        String written = writer.writeValueAsString(ev);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> read = objectMapper.readerFor(OBJECTREF).readValue(written);
        @SuppressWarnings("unchecked")
        Map<String, Object> readEvent = (Map<String, Object>) read.get("loghub.Event");
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) readEvent.get("@fields");
        Assert.assertEquals("1970-01-01T00:00:00.001Z", readEvent.get("@timestamp"));
        Assert.assertEquals(Map.of("ma", 1),  readEvent.get("@METAS"));
        Assert.assertEquals(a,  fields.get("a"));
        Assert.assertEquals(DatetimeProcessor.of("iso_nanos").print(now),  fields.get("b"));
    }

}
