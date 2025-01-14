package loghub.senders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import loghub.decoders.DecodeException;
import loghub.decoders.Decoder;
import loghub.decoders.Sflow;
import loghub.events.Event;
import loghub.receivers.Udp;

public class TestSflow {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.Sflow");
    }

    @Test
    public void testDecode() throws IOException, DecodeException {
        Udp.Builder udpBuilder = Udp.getBuilder();
        udpBuilder.setPort(6343);
        Decoder d = Sflow.getBuilder().build();
        d.configure(new Properties(new HashMap<>()), udpBuilder.build());
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
                d.decode(ConnectionContext.EMPTY, Unpooled.wrappedBuffer(p)).forEach(m -> {
                    Assert.assertTrue(m instanceof Event);
                    Event ev = (Event) m;
                    Assert.assertEquals(5, ev.getAtPath(VariablePath.of("observer", "version")));
                    Assert.assertTrue(ev.getAtPath(VariablePath.of("observer", "sequence_number")) instanceof Long);
                    if ("flow_sample".equals(ev.getAtPath(VariablePath.of("format")))) {
                        List<Map<?, ?>> samples = (List<Map<?, ?>>) ev.getAtPath(VariablePath.of("samples"));
                        Assert.assertTrue(samples.get(0).get("header") instanceof byte[]);
                    }
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
