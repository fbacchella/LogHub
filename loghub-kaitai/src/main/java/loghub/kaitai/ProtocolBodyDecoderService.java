package loghub.kaitai;

import java.util.Optional;

import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import loghub.kaitai.parsers.IcmpPacket;
import loghub.kaitai.parsers.ProtocolBody;
import loghub.kaitai.parsers.VrrpV3;

public class ProtocolBodyDecoderService extends KaitaiStreamDecoderService<ProtocolBody> {
    @Override
    public Optional<? extends KaitaiStruct> decode(ProtocolBody struct, KaitaiStream stream) {
        switch (struct.protocol()) {
        case ICMP:
            return Optional.of(new IcmpPacket(stream, struct));
        default:
            return Optional.empty();
        }
    }

    @Override
    public Class<ProtocolBody> getKind() {
        return ProtocolBody.class;
    }
}
