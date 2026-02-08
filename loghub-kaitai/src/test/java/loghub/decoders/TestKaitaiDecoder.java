package loghub.decoders;

import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import loghub.ConnectionContext;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.kaitai.KaitaiStreamDecoderService;
import loghub.kaitai.parsers.EthernetFrame;
import loghub.kaitai.parsers.Pcap;
import loghub.kaitai.parsers.Pcap.Packet;
import loghub.types.MacAddress;

class TestKaitaiDecoder {

    private final EventsFactory factory = new EventsFactory();

    public static class MockKaitaiStruct extends KaitaiStruct {
        public String visibleField = "visible";
        private String _internalField = "hidden";

        public MockKaitaiStruct(KaitaiStream _io) {
            super(_io);
        }
        
        public String visibleBean() { return visibleField; }
        public String _internalBean() { return _internalField; }
        @Override
        public KaitaiStruct _parent() { return null; }
    }

    @Test
    void testIgnoreInternalFields() throws DecodeException {
        KaitaiDecoder.Builder builder = new KaitaiDecoder.Builder();
        builder.setKaitaiStruct(MockKaitaiStruct.class.getName());
        KaitaiDecoder decoder = builder.build();

        byte[] data = "dummy".getBytes(StandardCharsets.UTF_8);
        
        Map<String, Object> result = decoder.decode(ConnectionContext.EMPTY, data)
                                           .toList().get(0);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.containsKey("visibleBean"));
    }

    @Test
    void testChargenUdp() throws IOException, DecodeException {
        KaitaiDecoder.Builder builder = new KaitaiDecoder.Builder();
        builder.setKaitaiStruct(EthernetFrame.class.getName());
        KaitaiDecoder decoder = builder.build();
        try(InputStream is = TestKaitaiDecoder.class.getResourceAsStream("/chargen-udp.pcap")) {
            ByteBufferKaitaiStream pcapcontent = new ByteBufferKaitaiStream(is.readAllBytes());
            Pcap pcap = new Pcap(pcapcontent);
            for (Packet i: pcap.packets()) {
                Map<String, Object> o = (Map<String, Object>) decoder.decodeObject(ConnectionContext.EMPTY, i._raw_body());
                Event ev = factory.newEvent();
                ev.putAll(o);
                Assertions.assertEquals("IPV4", ev.getAtPath(VariablePath.of("etherType")).toString());
                Assertions.assertInstanceOf(MacAddress.class, ev.getAtPath(VariablePath.of("dstMac")));
                Assertions.assertInstanceOf(MacAddress.class, ev.getAtPath(VariablePath.of("srcMac")));
                Assertions.assertInstanceOf(InetAddress.class, ev.getAtPath(VariablePath.of("body", "srcIpAddr")));
                Assertions.assertInstanceOf(InetAddress.class, ev.getAtPath(VariablePath.of("body", "dstIpAddr")));
            }
        }
    }

    @Test
    void testIcmp() throws IOException, DecodeException {
        KaitaiDecoder.Builder builder = new KaitaiDecoder.Builder();
        builder.setKaitaiStruct(EthernetFrame.class.getName());
        KaitaiDecoder decoder = builder.build();
        try(InputStream is = TestKaitaiDecoder.class.getResourceAsStream("/ipv4frags.pcap")) {
            ByteBufferKaitaiStream pcapcontent = new ByteBufferKaitaiStream(is.readAllBytes());
            Pcap pcap = new Pcap(pcapcontent);
            for (Packet i: pcap.packets()) {
                Map<String, Object> o = (Map<String, Object>) decoder.decodeObject(ConnectionContext.EMPTY, i._raw_body());
                Event ev = factory.newEvent();
                ev.putAll(o);
                Assertions.assertEquals("IPV4", ev.getAtPath(VariablePath.of("etherType")).toString());
                Assertions.assertInstanceOf(MacAddress.class, ev.getAtPath(VariablePath.of("dstMac")));
                Assertions.assertInstanceOf(MacAddress.class, ev.getAtPath(VariablePath.of("srcMac")));
                Assertions.assertInstanceOf(Inet4Address.class, ev.getAtPath(VariablePath.of("body", "srcIpAddr")));
                Assertions.assertInstanceOf(Inet4Address.class, ev.getAtPath(VariablePath.of("body", "dstIpAddr")));
                Assertions.assertEquals("ICMP", ev.getAtPath(VariablePath.of("body", "body", "protocol")).toString());
            }
        }
    }

    @Test
    void testResolve() {
        MockKaitaiStruct mock = new MockKaitaiStruct(null);
        byte[] raw = new byte[]{1, 2, 3};
        // services is initialized at class loading
        // Since there are no services registered, resolve should return rawBody
        Optional<?> result = KaitaiStreamDecoderService.resolve(mock, new ByteBufferKaitaiStream(raw));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testReset() {
        // Just verify that reset can be called without exception
        // Since we don't have a way to easily change the classpath/services at runtime here
        // this is mostly a smoke test.
        KaitaiStreamDecoderService.reset();
        
        MockKaitaiStruct mock = new MockKaitaiStruct(null);
        byte[] raw = new byte[]{1, 2, 3};
        Optional<?> result = KaitaiStreamDecoderService.resolve(mock, new ByteBufferKaitaiStream(raw));
        Assertions.assertTrue(result.isEmpty());
    }
}
