package loghub.decoders.netflow;

import io.netty.buffer.ByteBuf;
import loghub.Decoder.DecodeException;

public class PacketFactory {

    private PacketFactory() {
    }

    public static NetflowPacket parsePacket(ByteBuf bbuf) throws DecodeException {
        bbuf.markReaderIndex();
        short version = bbuf.readShort();
        bbuf.resetReaderIndex();
        switch(version) {
        case 5:
            return new Netflow5Packet(bbuf);
        case 9:
            return new Netflow9Packet(bbuf);
        case 10:
            return new IpfixPacket(bbuf);
        default:
            throw new DecodeException("Unsupported netflow/IPFIX packet version: " + version);
        }
    }

}
