package loghub.netflow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;

import io.netty.buffer.ByteBuf;
import loghub.Decoder.DecodeException;

public class PacketFactory {
    
    static public final String TYPEKEY = "_type";

    private static final IpfixInformationElements ipfixtypes;
    static {
        try {
            ipfixtypes = new IpfixInformationElements();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PacketFactory() {
    }

    public static NetflowPacket parsePacket(InetAddress remoteAddr, ByteBuf bbuf) throws DecodeException {
        bbuf.markReaderIndex();
        short version = bbuf.readShort();
        bbuf.resetReaderIndex();
        switch(version) {
        case 5:
            return new Netflow5Packet(bbuf);
        case 9:
            return new Netflow9Packet(remoteAddr, bbuf, ipfixtypes);
        case 10:
            return new IpfixPacket(remoteAddr, bbuf, ipfixtypes);
        default:
            throw new DecodeException("Unsupported netflow/IPFIX packet version: " + version);
        }
    }

}
