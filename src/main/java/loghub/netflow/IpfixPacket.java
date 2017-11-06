package loghub.netflow;

import java.net.InetAddress;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public class IpfixPacket extends TemplateBasePacket implements NetflowPacket {

    private static final Function<ByteBuf, HeaderInfo> headerreder = (i) -> {
        TemplateBasePacket.HeaderInfo hi = new TemplateBasePacket.HeaderInfo();
        hi.length =  Short.toUnsignedInt(i.readShort());
        return hi;
    };

    public IpfixPacket(InetAddress remoteAddr, ByteBuf bbuf, IpfixInformationElements ipfixtypes) {
        super(remoteAddr, bbuf, headerreder, ipfixtypes);
    }

    @Override
    public int getVersion() {
        return 10;
    }

}
