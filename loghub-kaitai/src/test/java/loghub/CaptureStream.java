package loghub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.stream.Stream;

import io.kaitai.struct.ByteBufferKaitaiStream;
import loghub.kaita.parsers.EthernetFrame;
import loghub.kaita.parsers.Ipv4Packet;
import loghub.kaita.parsers.Pcap;
import loghub.kaita.parsers.TcpSegment;
import loghub.kaita.parsers.UdpDatagram;

public class CaptureStream {

    public static Stream<byte[]> readUdpStream(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.write(in.readAllBytes());
            ByteBufferKaitaiStream pcapcontent = new ByteBufferKaitaiStream(out.toByteArray());
            Pcap pcap = new Pcap(pcapcontent);
            return pcap.packets().stream()
                              .map(p -> (EthernetFrame)p.body())
                              .map(p -> (Ipv4Packet)p.body())
                              .map(Ipv4Packet::body)
                              .map(p -> (UdpDatagram)p.body())
                              .map(UdpDatagram::body);
        }
    }

    public record TcpQuadruplet(InetAddress srcAddr, int srcPort, InetAddress dstAddress, int dstPort) {
        @Override
        public String toString() {
            return "%s:%s->%s:%s".formatted(srcAddr.getHostAddress(), srcPort, dstAddress.getHostAddress(), dstPort);
        }
    }

    public record TcpSegmentPayload(TcpQuadruplet quad, byte[] body) {

    }

    public static TcpSegmentPayload segmentInfo(TcpSegment seg) {
        try {
            Ipv4Packet packet = (Ipv4Packet) seg._parent();
            TcpQuadruplet quad = new TcpQuadruplet(InetAddress.getByAddress(packet.srcIpAddr()), seg.srcPort(), InetAddress.getByAddress(packet.dstIpAddr()), seg.dstPort());
            //System.err.println(quad);
            //System.err.format("%s %s %s %s%n", InetAddress.getByAddress(packet.srcIpAddr()).getHostAddress(), seg.srcPort(), InetAddress.getByAddress(packet.dstIpAddr()), seg.dstPort());
            return new TcpSegmentPayload(quad, seg.body());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<TcpSegmentPayload> readTcpStream(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.write(in.readAllBytes());
            ByteBufferKaitaiStream pcapcontent = new ByteBufferKaitaiStream(out.toByteArray());
            Pcap pcap = new Pcap(pcapcontent);
            return pcap.packets().stream()
                           .map(p -> (EthernetFrame)p.body())
                           .map(p -> (Ipv4Packet)p.body())
                           .map(Ipv4Packet::body)
                           .map(p -> (TcpSegment)p.body())
                           .map(CaptureStream::segmentInfo);
        }
    }

}
