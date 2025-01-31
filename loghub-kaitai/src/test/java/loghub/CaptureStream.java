package loghub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import io.kaitai.struct.ByteBufferKaitaiStream;
import kaitai.EthernetFrame;
import kaitai.Ipv4Packet;
import kaitai.Pcap;
import kaitai.UdpDatagram;

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

}
