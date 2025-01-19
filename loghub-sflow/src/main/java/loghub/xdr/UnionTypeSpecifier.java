package loghub.xdr;

import java.io.IOException;
import java.net.InetAddress;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class UnionTypeSpecifier extends TypeSpecifier<String> {

    public static UnionTypeSpecifier of(String name) {
        switch (name) {
        case "address":
            return new UnionTypeSpecifier(name, UnionTypeSpecifier::readIpAddress);
        default:
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
