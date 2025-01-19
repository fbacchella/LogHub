package loghub.xdr;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class UnionTypeSpecifier extends TypeSpecifier<String> {

    private static final Logger logger = LogManager.getLogger();

    public static UnionTypeSpecifier of(String name) {
        switch (name) {
        case "address":
            return new UnionTypeSpecifier(name, UnionTypeSpecifier::readIpAddress);
        case "as_path_type":
            return new UnionTypeSpecifier(name, UnionTypeSpecifier::readAsPathType);
        default:
            logger.warn("Unhandled union {}", name);
            return new UnionTypeSpecifier("Unspecified/" + name, b -> thrower(name));
        }
    }

    private static Object thrower(String name) {
        throw new UnsupportedOperationException(String.format("Union %s must be implemented", name));
    }

    private static  InetAddress readIpAddress(ByteBuf bbuf) throws IOException {
        int adressType = bbuf.readInt();
        byte[] ipAddrBuffer;

        if (adressType == 1) {
            ipAddrBuffer = new byte[4];
        } else if (adressType == 2) {
            ipAddrBuffer = new byte[16];
        } else {
            throw new IOException("Unknown address type in packet " + adressType);
        }
        bbuf.readBytes(ipAddrBuffer);
        return InetAddress.getByAddress(ipAddrBuffer);
    }

    private static Map<String, List<Long>> readAsPathType(ByteBuf bbuf) throws IOException {
        int asPathSegmentType = bbuf.readInt();
        int size = Math.toIntExact(bbuf.readUnsignedInt());
        List<Long> destination = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            destination.add(bbuf.readUnsignedInt());
        }
        String pathTypeName;
        if (asPathSegmentType == 1) {
            pathTypeName = "as_set";
        } else if (asPathSegmentType == 2) {
            pathTypeName = "as_sequence";
        } else {
            throw new IOException("Unknown AS type in packet " + asPathSegmentType);
        }
        Map<String, List<Long>> data = new HashMap<>();
        data.put(pathTypeName, destination);
        return data;
    }

    private final ReadType<?> reader;

    private UnionTypeSpecifier(String name, ReadType<?> reader) {
        super(name);
        this.reader = reader;
    }

    @Override
    public String getType() {
        return getName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> O read(ByteBuf b) throws IOException {
        return (O) reader.read(b);
    }

    @Override
    public String toString() {
        return "Union/" + getName();
    }

}
