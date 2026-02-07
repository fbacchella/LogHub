package loghub.kaita;

import java.util.Optional;

import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import loghub.kaita.parsers.IcmpPacket;
import loghub.kaita.parsers.ProtocolBody;

public class ProtocolBodyDecoderService extends KaitaiStreamDecoderService<ProtocolBody> {
    @Override
    public Optional<? extends KaitaiStruct> decode(ProtocolBody struct, KaitaiStream stream) {
        switch (struct.protocol()) {
        case ICMP:
            return Optional.of(new IcmpPacket(stream));
        default:
            return Optional.empty();
        }
    }

    @Override
    public Class<ProtocolBody> getKind() {
        return ProtocolBody.class;
    }
}
