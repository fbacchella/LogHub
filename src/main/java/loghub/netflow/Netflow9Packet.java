package loghub.netflow;

import java.net.InetAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public class Netflow9Packet extends TemplateBasePacket implements NetflowPacket {

    private static final Function<ByteBuf, HeaderInfo> headerreder = (i) -> {
        TemplateBasePacket.HeaderInfo hi = new TemplateBasePacket.HeaderInfo();
        hi.count =  Short.toUnsignedInt(i.readShort());
        hi.sysUpTime = Integer.toUnsignedLong(i.readInt());
        return hi;
    };

    private final Duration sysUpTime;

    public Netflow9Packet(InetAddress remoteAddr, ByteBuf bbuf, IpfixInformationElements nf9types) {
        super(remoteAddr, bbuf, headerreder, nf9types);
        sysUpTime = Duration.of(header.sysUpTime, ChronoUnit.MILLIS);
    }

    @Override
    public int getVersion() {
        return 9;
    }

    public Duration getSysUpTime() {
        return sysUpTime;
    }

}
