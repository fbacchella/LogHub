package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.Unpooled;
import loghub.BeanChecks;
import loghub.kaitai.CaptureStream;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.receivers.Udp;

public class TestSflow {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.decoders.Sflow", "loghub.sflow", "loghub.xdr");
    }

    @Test
    public void testDecode() throws IOException {
        EventsFactory factory = new EventsFactory();
        Udp.Builder udpBuilder = Udp.getBuilder();
        udpBuilder.setPort(6343);

        List<String> xdrs = new ArrayList<>();
        xdrs.add("bad");
        Sflow.Builder sbuilder = Sflow.getBuilder();
        sbuilder.setXdrPaths(xdrs.toArray(String[]::new));
        sbuilder.setFactory(factory);
        Decoder d = sbuilder.build();
        d.configure(new Properties(new HashMap<>()), udpBuilder.build());
        AtomicInteger failedPackets = new AtomicInteger();

        try (InputStream in = TestSflow.class.getClassLoader().getResourceAsStream("sflow.pcap")) {
            CaptureStream.readUdpStream(in).forEach(p -> {
                try {
                    d.decode(ConnectionContext.EMPTY, Unpooled.wrappedBuffer(p)).forEach(m -> {
                        Assert.assertTrue(m instanceof Event);
                        Event ev = (Event) m;
                        Assert.assertEquals(5, ev.getAtPath(VariablePath.of("observer", "version")));
                        Assert.assertTrue(ev.getAtPath(VariablePath.of("observer", "sequence_number")) instanceof Long);
                    });
                } catch (DecodeException | RuntimeException ex) {
                    System.err.println(failedPackets.get()  + " -> " + Helpers.resolveThrowableException(ex));
                    Assert.fail(Helpers.resolveThrowableException(ex));
                }
            });
        }

    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Sflow"
        );
    }

}
