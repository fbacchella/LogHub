package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.netty.buffer.Unpooled;
import kaitai.EthernetFrame;
import kaitai.Ipv4Packet;
import kaitai.Pcap;
import kaitai.UdpDatagram;
import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
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
    public void testDecode() throws IOException, DecodeException {
        Udp.Builder udpBuilder = Udp.getBuilder();
        udpBuilder.setPort(6343);

        List<String> xdrs = new ArrayList<>();
        xdrs.add("bad");
        Sflow.Builder sbuilder = Sflow.getBuilder();
        sbuilder.setXdrPaths(xdrs.toArray(String[]::new));
        Decoder d = sbuilder.build();
        d.configure(new Properties(new HashMap<>()), udpBuilder.build());
        AtomicInteger failedPackets = new AtomicInteger();
        List<byte[]> paquets;
        try (InputStream in = TestSflow.class.getClassLoader().getResourceAsStream("sflow.pcap");
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.write(in.readAllBytes());
            ByteBufferKaitaiStream pcapcontent = new ByteBufferKaitaiStream(out.toByteArray());
            Pcap pcap = new Pcap(pcapcontent);
            paquets = pcap.packets().stream()
                              .map(p -> (EthernetFrame)p.body())
                              .map(p -> (Ipv4Packet)p.body())
                              .map(Ipv4Packet::body)
                              .map(p -> (UdpDatagram)p.body())
                              .map(UdpDatagram::body)
                              .collect(Collectors.toList());
            for (byte[] p: paquets) {
                failedPackets.incrementAndGet();
                d.decode(ConnectionContext.EMPTY, Unpooled.wrappedBuffer(p)).forEach(m -> {
                    Assert.assertTrue(m instanceof Event);
                    Event ev = (Event) m;
                    Assert.assertEquals(5, ev.getAtPath(VariablePath.of("observer", "version")));
                    Assert.assertTrue(ev.getAtPath(VariablePath.of("observer", "sequence_number")) instanceof Long);
                });
            }
        }

    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Sflow"
        );
    }

}
