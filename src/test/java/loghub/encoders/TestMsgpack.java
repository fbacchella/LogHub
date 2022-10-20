package loghub.encoders;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.databind.ObjectReader;

import loghub.ConnectionContext;
import loghub.events.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import loghub.jackson.MsgpackTimeDeserializer;
import loghub.senders.InMemorySender;

public class TestMsgpack {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testParsing() {
        ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { forwardEvent: true}}}", i -> i.output());
    }

    @Test
    public void testParsingFailedBadBean() {
        RecognitionException ex = Assert.assertThrows(RecognitionException.class, () -> {
            ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { notbean: 1}}}", i -> i.output());
        });
        Assert.assertEquals("Unknown bean 'notbean' for loghub.encoders.Msgpack", ex.getMessage());
    }

    @Test
    public void testParsingFailedbadType() {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> {
            ConfigurationTools.parseFragment("output { loghub.senders.Stdout { encoder: loghub.encoders.Msgpack { forwardEvent: 1}}}", i -> i.output());
        });
        Assert.assertEquals("no viable alternative at input '1'", ex.getMessage());
    }

    @Test
    public void testCodeDecode() throws IOException, EncodeException {
        Msgpack.Builder builder = Msgpack.getBuilder();
        builder.setForwardEvent(true);
        Msgpack encoder = builder.build();
        InMemorySender.Builder sender = InMemorySender.getBuilder();
        Assert.assertTrue(encoder.configure(new Properties(Collections.emptyMap()), sender.build()));
        byte[] data = encoder.encode(Stream.of(factory.newEvent(), factory.newEvent()));

        ExtensionTypeCustomDeserializers extTypeCustomDesers = new ExtensionTypeCustomDeserializers();
        extTypeCustomDesers.addCustomDeser((byte) -1, new MsgpackTimeDeserializer());
        ObjectReader reader = JacksonBuilder.get()
                                            .setFactory(new MessagePackFactory().setExtTypeCustomDesers(extTypeCustomDesers))
                                            .getReader();
        List<Map<String, Object>> read = reader.readValue(data, 0, data.length);
        Assert.assertEquals(2 , read.size());
        read.forEach( i-> {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = (Map<String, Object>) i.get(Event.class.getCanonicalName());
            Instant timestamp = (Instant) event.get(Event.TIMESTAMPKEY);
            Assert.assertNotNull(timestamp);
        });
    }

}
