package loghub.decoders.netflow;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class Netflow5Packet implements NetflowPacket {

    private final int count;
    @SuppressWarnings("unused")
    private final long sysUpTime;
    private final Instant exportTime;
    private final long sequenceNumber;
    private final int type;
    private final Integer id;
    List<Map<String, Object>> records;


    public Netflow5Packet(ByteBuf bbuf) {
        //Skip version
        short version = bbuf.readShort();
        if (version != 5) {
            throw new RuntimeException("Invalid version");
        }
        count = Short.toUnsignedInt(bbuf.readShort());
        sysUpTime = Integer.toUnsignedLong(bbuf.readInt());
        long exportSecs = Integer.toUnsignedLong(bbuf.readInt());
        long exportNano = Integer.toUnsignedLong(bbuf.readInt());
        exportTime = Instant.ofEpochSecond(exportSecs, exportNano);
        sequenceNumber = Integer.toUnsignedLong(bbuf.readInt());
        type = Byte.toUnsignedInt(bbuf.readByte());
        id = Byte.toUnsignedInt(bbuf.readByte());
        //Sampling interval
        bbuf.readShort();

        records = new ArrayList<>(count);
        byte[] addrbuffer = new byte[4];
        for(int i = 0; i < count; i++) {
            try {
                Map<String, Object> record = new HashMap<>(20);
                bbuf.readBytes(addrbuffer);
                record.put("srcaddr", InetAddress.getByAddress(addrbuffer));
                bbuf.readBytes(addrbuffer);
                record.put("dstaddr", InetAddress.getByAddress(addrbuffer));
                bbuf.readBytes(addrbuffer);
                record.put("nexthop", InetAddress.getByAddress(addrbuffer));
                record.put("input", Short.toUnsignedInt(bbuf.readShort()));
                record.put("output", Short.toUnsignedInt(bbuf.readShort()));
                record.put("dPkts", Integer.toUnsignedLong(bbuf.readInt()));
                record.put("dOctets", Integer.toUnsignedLong(bbuf.readInt()));
                record.put("first", Integer.toUnsignedLong(bbuf.readInt()));
                record.put("last", Integer.toUnsignedLong(bbuf.readInt()));
                record.put("srcport", Short.toUnsignedInt(bbuf.readShort()));
                record.put("dstport", Short.toUnsignedInt(bbuf.readShort()));
                bbuf.readByte();  // some padding;
                record.put("tcp_flags", bbuf.readByte());
                record.put("prot", Byte.toUnsignedInt(bbuf.readByte()));
                record.put("tos", Byte.toUnsignedInt(bbuf.readByte()));
                record.put("src_as", Short.toUnsignedInt(bbuf.readShort()));
                record.put("dst_as", Short.toUnsignedInt(bbuf.readShort()));
                record.put("src_mask", Byte.toUnsignedInt(bbuf.readByte()));
                record.put("dst_mask", Byte.toUnsignedInt(bbuf.readByte()));
                bbuf.readShort();  // some padding;
                records.add(record);
            } catch (UnknownHostException e) {
                continue;
            } catch (IndexOutOfBoundsException e) {
                break;
            } catch (Exception e) {
                continue;
            }
        }

    }

    @Override
    public int getVersion() {
        return 5;
    }

    @Override
    public int getLength() {
        return count;
    }

    @Override
    public Instant getExportTime() {
        return exportTime;
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public List<Map<String, Object>> getRecords() {
        return records;
    }

}
