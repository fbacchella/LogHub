package loghub.decoders.netflow;

import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public class Netflow9Packet extends TemplateBasePacket implements NetflowPacket {

    private static final Function<ByteBuf, HeaderInfo> headerreder = (i) -> {
        TemplateBasePacket.HeaderInfo hi = new TemplateBasePacket.HeaderInfo();
        hi.count =  Short.toUnsignedInt(i.readShort());
        hi.sysUpTime = Integer.toUnsignedLong(i.readInt());
        return hi;
    };

    public Netflow9Packet(ByteBuf bbuf) {
        //bbuf.setIndex(8, bbuf.writerIndex())
        super(bbuf, headerreder);
    }

    @Override
    public int getVersion() {
        return 9;
    }

}
