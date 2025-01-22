package loghub.netflow;

import java.net.InetAddress;

import io.netty.buffer.ByteBuf;

public class IpfixPacket extends TemplateBasedPacket implements NetflowPacket {

    public IpfixPacket(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry) {
        super(remoteAddr, bbuf, registry);
    }

    @Override
    protected HeaderInfo readHeader(ByteBuf bbuf) {
        TemplateBasedPacket.HeaderInfo hi = new TemplateBasedPacket.HeaderInfo();
        hi.length =  Short.toUnsignedInt(bbuf.readShort());
        return hi;
    }

    @Override
    public int getVersion() {
        return 10;
    }

}
