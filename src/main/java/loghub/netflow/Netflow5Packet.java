package loghub.netflow;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import loghub.netflow.TemplateBasePacket.TemplateType;

public class Netflow5Packet implements NetflowPacket {

    private static final Logger logger = LogManager.getLogger();

    private final int count;
    private final Duration sysUpTime;
    private final Instant exportTime;
    private final long sequenceNumber;
    private final int type;
    private final Integer id;
    private final short samplingInterval;
    private final byte samplingMode;
    List<Map<String, Object>> records;

    public Netflow5Packet(ByteBuf bbuf) {
        //Skip version
        short version = bbuf.readShort();
        if (version != 5) {
            throw new RuntimeException("Invalid version");
        }
        count = Short.toUnsignedInt(bbuf.readShort());
        if (count > 0) {
            logger.trace("{} records expected", count);
        }
        long sysUpTimeValue = Integer.toUnsignedLong(bbuf.readInt());
        sysUpTime = Duration.of(sysUpTimeValue, ChronoUnit.MILLIS);
        long exportSecs = Integer.toUnsignedLong(bbuf.readInt());
        long exportNano = Integer.toUnsignedLong(bbuf.readInt());
        exportTime = Instant.ofEpochSecond(exportSecs, exportNano);
        sequenceNumber = Integer.toUnsignedLong(bbuf.readInt());
        type = Byte.toUnsignedInt(bbuf.readByte());
        id = Byte.toUnsignedInt(bbuf.readByte());
        //Sampling interval
        short samplingIntervalBuffer = bbuf.readShort();
        samplingInterval = (short) (samplingIntervalBuffer & 0x3FF);
        samplingMode = (byte) (samplingIntervalBuffer >> 14);

        records = new ArrayList<>(count);
        byte[] addrbuffer = new byte[4];
        for(int i = 0; i < count; i++) {
            try {
                Map<String, Object> nfRecord = new HashMap<>(20);
                bbuf.readBytes(addrbuffer);
                nfRecord.put(PacketFactory.TYPEKEY, TemplateType.Records);
                nfRecord.put("srcaddr", InetAddress.getByAddress(addrbuffer));
                bbuf.readBytes(addrbuffer);
                nfRecord.put("dstaddr", InetAddress.getByAddress(addrbuffer));
                bbuf.readBytes(addrbuffer);
                nfRecord.put("nexthop", InetAddress.getByAddress(addrbuffer));
                nfRecord.put("input", Short.toUnsignedInt(bbuf.readShort()));
                nfRecord.put("output", Short.toUnsignedInt(bbuf.readShort()));
                nfRecord.put("dPkts", Integer.toUnsignedLong(bbuf.readInt()));
                nfRecord.put("dOctets", Integer.toUnsignedLong(bbuf.readInt()));
                nfRecord.put("first", Integer.toUnsignedLong(bbuf.readInt()));
                nfRecord.put("last", Integer.toUnsignedLong(bbuf.readInt()));
                nfRecord.put("srcport", Short.toUnsignedInt(bbuf.readShort()));
                nfRecord.put("dstport", Short.toUnsignedInt(bbuf.readShort()));
                bbuf.readByte();  // some padding;
                nfRecord.put("tcp_flags", bbuf.readByte());
                nfRecord.put("prot", Byte.toUnsignedInt(bbuf.readByte()));
                nfRecord.put("tos", Byte.toUnsignedInt(bbuf.readByte()));
                nfRecord.put("src_as", Short.toUnsignedInt(bbuf.readShort()));
                nfRecord.put("dst_as", Short.toUnsignedInt(bbuf.readShort()));
                nfRecord.put("src_mask", Byte.toUnsignedInt(bbuf.readByte()));
                nfRecord.put("dst_mask", Byte.toUnsignedInt(bbuf.readByte()));
                bbuf.readShort();  // some padding;
                records.add(nfRecord);
            } catch (IndexOutOfBoundsException e) {
                break;
            } catch (Exception e) {

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

    /**
     * @return the sampling_mode
     */
    public byte getSamplingMode() {
        return samplingMode;
    }

    /**
     * @return the sampling_interval
     */
    public short getSamplingInterval() {
        return samplingInterval;
    }

    /**
     * @return the type
     */
    public int getEngineType() {
        return type;
    }

    public Duration getSysUpTime() {
        return sysUpTime;
    }

}
