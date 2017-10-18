package loghub.decoders.netflow;

import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public class IpfixPacket extends TemplateBasePacket implements NetflowPacket {

    private static final Function<ByteBuf, HeaderInfo> headerreder = (i) -> {
        TemplateBasePacket.HeaderInfo hi = new TemplateBasePacket.HeaderInfo();
        hi.length =  Short.toUnsignedInt(i.readShort());
        return hi;
    };
    
    public IpfixPacket(ByteBuf bbuf) {
        super(bbuf, headerreder);
    }

    @Override
    public int getVersion() {
        return 10;
    }

}
