package loghub.encoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BuildableConnectionContext.GenericConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.cbor.CborParser.CborParserFactory;
import loghub.cbor.LogHubEventTagHandler;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestCbor {

    private static Logger logger;
    private final EventsFactory eventsFactory = new EventsFactory();
    private final CborParserFactory cborFactory = new CborParserFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, Cbor.class.getName());
    }

    @Before
    public void initCbor() {
        cborFactory.setCustomHandling(LogHubEventTagHandler.EVENT_TAG, LogHubEventTagHandler.eventParser(eventsFactory), null);
    }

    public String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(String.format("%02X ", b)); // ou "%02x" pour minuscule
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    @Test
    public void testRoundRobin() throws EncodeException, DecodeException {
        loghub.encoders.Cbor encoder = loghub.encoders.Cbor.getBuilder().build();
        loghub.decoders.Cbor.Builder dbuilder = loghub.decoders.Cbor.getBuilder();
        dbuilder.setEventsFactory(eventsFactory);
        loghub.decoders.Cbor decoders = dbuilder.build();
        Event e = eventsFactory.newEvent();
        e.setTimestamp(Instant.EPOCH);
        e.putMeta("ameta", 1);
        UUID ruuid = UUID.randomUUID();
        URI uri = URI.create("https://github.com/fbacchella/LogHub");
        e.put("uuid", ruuid);
        e.put("uri", uri);
        e.putAtPath(VariablePath.of("a", "b"), InetAddress.getLoopbackAddress());
        byte[] result = encoder.encode(e);
        List<Event> received = decoders.decode(new GenericConnectionContext("laddr", "raddr"), result).map(Event.class::cast).toList();
        Assert.assertEquals(1, received.size());
        Event event = received.get(0);
        Assert.assertEquals(ruuid, event.get("uuid"));
        Assert.assertEquals(uri, event.get("uri"));
        Assert.assertEquals(InetAddress.getLoopbackAddress(), event.getAtPath(VariablePath.of("a", "b")));
        Assert.assertEquals(new Date(0), event.getTimestamp());
        Assert.assertEquals(1, event.getMeta("ameta"));
        Assert.assertFalse(event.getConnectionContext() instanceof GenericConnectionContext);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.encoders.Cbor"
                , BeanChecks.BeanInfo.build("classLoader", ClassLoader.class)
        );
    }

}
