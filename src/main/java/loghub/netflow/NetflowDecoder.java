package loghub.netflow;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.decoders.Decoder;

@BuilderClass(NetflowDecoder.Builder.class)
public class NetflowDecoder extends Decoder {

    public static class Builder extends Decoder.Builder<NetflowDecoder> {
        @Override
        public NetflowDecoder build() {
            return new NetflowDecoder(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private NetflowDecoder(loghub.decoders.Decoder.Builder<? extends Decoder> builder) {
        super(builder);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        InetAddress addr;
        if (ctx instanceof IpConnectionContext) {
            addr = ((IpConnectionContext)ctx).getRemoteAddress().getAddress();
            NetflowPacket packet = PacketFactory.parsePacket(addr, bbuf);
            Map<String, Object> ev = new HashMap<>();
            ev.put(Event.TIMESTAMPKEY, Date.from(packet.getExportTime()));
            ev.put("sequenceNumber", packet.getSequenceNumber());
            switch (packet.getVersion()) {
            case 5:
                Netflow5Packet packet5 = (Netflow5Packet) packet;
                ev.put("engine_type", packet5.getEngineType());
                ev.put("sampling_interval", packet5.getSamplingInterval());
                ev.put("sampling_mode", packet5.getSamplingMode());
                ev.put("SysUptime", packet5.getSysUpTime());
                break;
            case 9:
                ev.put("SysUptime", ((Netflow9Packet)packet).getSysUpTime());
                break;
            case 10:
                break;
            default:
                throw new UnsupportedOperationException();
            }
            ev.put("version", packet.getVersion());
            ev.put("records", packet.getRecords());
            return ev;
        }
        return null;
    }

}
