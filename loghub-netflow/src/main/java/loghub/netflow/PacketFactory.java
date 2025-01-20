package loghub.netflow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.decoders.DecodeException;

public class PacketFactory {

    public static final String TYPEKEY = "_type";

    private static final IpfixInformationElements ipfixtypes;
    static {
        try {
            ipfixtypes = new IpfixInformationElements();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final Map<TemplateId, Map<Integer, Template>> templates = new HashMap<>();

    public PacketFactory() {
    }

    public NetflowPacket parsePacket(InetAddress remoteAddr, ByteBuf bbuf) throws DecodeException {
        bbuf.markReaderIndex();
        short version = bbuf.readShort();
        bbuf.resetReaderIndex();
        switch(version) {
        case 5:
            return new Netflow5Packet(bbuf);
        case 9:
            return new Netflow9Packet(remoteAddr, bbuf, ipfixtypes, templates);
        case 10:
            return new IpfixPacket(remoteAddr, bbuf, ipfixtypes, templates);
        default:
            throw new DecodeException("Unsupported netflow/IPFIX packet version: " + version);
        }
    }

}
