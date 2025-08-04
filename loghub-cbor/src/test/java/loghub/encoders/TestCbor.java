package loghub.encoders;

import java.beans.IntrospectionException;
import java.net.InetAddress;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BuildableConnectionContext.GenericConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.decoders.DecodeException;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestCbor {

    private static Logger logger;
    private final EventsFactory eventsFactory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, Cbor.class.getName());
    }

    @Test
    public void testRoundRobin() throws EncodeException, DecodeException {
        Cbor.Builder ebuilder = Cbor.getBuilder();
        ebuilder.setForwardEvent(true);
        loghub.encoders.Cbor encoder = ebuilder.build();
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
        Event event = received.getFirst();
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
