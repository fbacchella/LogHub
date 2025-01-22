package loghub.netflow;

import java.net.InetAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class Netflow9Packet extends TemplateBasedPacket implements NetflowPacket {

    private final Duration sysUpTime;

    public Netflow9Packet(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry) {
        super(remoteAddr, bbuf, registry);
        sysUpTime = Duration.of(header.sysUpTime, ChronoUnit.MILLIS);
    }

    @Override
    public int getVersion() {
        return 9;
    }

    @Override
    protected HeaderInfo readHeader(ByteBuf bbuf) {
        TemplateBasedPacket.HeaderInfo hi = new TemplateBasedPacket.HeaderInfo();
        hi.count =  Short.toUnsignedInt(bbuf.readShort());
        hi.sysUpTime = Integer.toUnsignedLong(bbuf.readInt());
        return hi;
    }

}
