package loghub.netflow;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.Event;
import loghub.IpConnectionContext;

public class NetflowDecoder extends Decoder {

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
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
