package loghub.kaitai;

import java.util.Optional;

import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import loghub.kaitai.parsers.IcmpPacket;
import loghub.kaitai.parsers.Igmp;
import loghub.kaitai.parsers.Ipv4Packet;
import loghub.kaitai.parsers.Ipv6Packet;
import loghub.kaitai.parsers.ProtocolBody;
import loghub.kaitai.parsers.Vrrp;

public class ProtocolBodyDecoderService extends KaitaiStreamDecoderService<ProtocolBody> {
    @Override
    public Optional<? extends KaitaiStruct> decode(ProtocolBody struct, KaitaiStream stream) {
        switch (struct.protocol()) {
        case ICMP:
            return Optional.of(new IcmpPacket(stream, struct));
        case VRRP:
            int version = -1;
            if (struct._parent() instanceof Ipv4Packet ipv4Packet) {
                version = ipv4Packet.version();
            } else if (struct._parent() instanceof Ipv6Packet ipv6Packet) {
                version = (int)ipv6Packet.version();
            }
            return Optional.of(new Vrrp(stream, version));
        case IGMP:
            return Optional.of(new Igmp(stream, struct));
        default:
            return Optional.empty();
        }
    }

    @Override
    public Class<ProtocolBody> getKind() {
        return ProtocolBody.class;
    }
}
