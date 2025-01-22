package loghub.netflow;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import loghub.Helpers;
import lombok.Getter;

public abstract class TemplateBasedPacket implements NetflowPacket {

    private static final Logger logger = LogManager.getLogger();

    protected static class HeaderInfo {
        int count = -1;
        int length = -1;
        long sysUpTime = 0;
    }

    protected final HeaderInfo header;
    protected final int sourceId;
    @Getter
    protected final Instant exportTime;
    @Getter
    protected final long sequenceNumber;
    protected final int length;
    protected final int count;
    @Getter
    private final List<Map<String, Object>> records;
    @Getter
    private final boolean withFailure = false;

    protected TemplateBasedPacket(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry) {
        short version = bbuf.readShort();
        if (version < 9) {
            throw new RuntimeException("Invalid version");
        }
        header = readHeader(bbuf);
        this.count = header.count;
        this.length = header.length;

        if (count > 0) {
            logger.trace("{} records expected", count);
        }
        exportTime = Instant.ofEpochSecond(Integer.toUnsignedLong(bbuf.readInt()));
        sequenceNumber = Integer.toUnsignedLong(bbuf.readInt());
        sourceId = bbuf.readInt();
        // If length is non zero, an Ipfix packet
        if (length > 0) {
            bbuf = bbuf.readSlice(length - 16);
        }
        int flowSetSeen = 0;
        int flowSeen = 0;
        List<Map<String, Object>> tmpRecords = new ArrayList<>();
        while (bbuf.isReadable()) {
            try {
                flowSeen += readSet(remoteAddr, bbuf, registry, tmpRecords);
                ++flowSetSeen;
            } catch (RuntimeException e) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Failed reading flow set {}", flowSetSeen);
            }
        }
        records = List.copyOf(tmpRecords);
        if (count > 0 && flowSetSeen > count) {
            logger.debug("Too much records seen: {}/{}/{}", flowSeen, flowSetSeen, count);
        } else if (flowSetSeen < count) {
            logger.debug("Not enough records: {}/{}/{}", flowSeen, flowSetSeen, count);
        }
    }

    protected abstract HeaderInfo readHeader(ByteBuf bbuf);

    protected int readSet(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry, List<Map<String, Object>> records) {
        Template.TemplateId key = new Template.TemplateId(remoteAddr, sourceId);
        int flowSetId = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        logger.trace("flow set id {}", flowSetId);
        switch (flowSetId) {
        case 0: // Netflow v9 Template FlowSet
            return registry.readTemplateSet(key, bbuf.readSlice(length - 4), false);
        case 1: // Netflow v9 Option Template FlowSet
            return registry.readOptionsTemplateNetflowSet(key, bbuf.readSlice(length - 4));
        case 2: // IPFIX Template FlowSet
            return registry.readTemplateSet(key, bbuf.readSlice(length - 4), true);
        case 3: // IPFIX Option Template FlowSet
            return registry.readOptionsTemplateIpfixSet(key, bbuf.readSlice(length - 4));
        default:
            return readDataSet(key, bbuf.readSlice(length - 4), flowSetId, registry, records);
        }
    }

    protected int readDataSet(Template.TemplateId key, ByteBuf bbuf, int flowSetId, NetflowRegistry registry, List<Map<String, Object>> records) {
        return registry.getTemplate(key, flowSetId)
                       .stream()
                       .mapToInt(t -> readDataSet(t, bbuf, registry, records))
                       .sum();
    }

    private int readDataSet(Template tpl, ByteBuf bbuf, NetflowRegistry registry, List<Map<String, Object>> records) {
        int recordCount = 0;
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(4)) {
            Map<String, Object> record = new HashMap<>(tpl.getSizes());
            logger.trace("  data");
            for (int i = 0; i < tpl.getSizes(); i++) {
                int type;
                int fieldSize = 0;
                try {
                    type = tpl.types.get(i);
                    fieldSize = tpl.getSize(i);
                    if (fieldSize == 65535) {
                        fieldSize = Byte.toUnsignedInt(bbuf.readByte());
                        if (fieldSize == 255) {
                            fieldSize = Short.toUnsignedInt(bbuf.readShort());
                        }
                    }
                    ByteBuf content = bbuf.readSlice(fieldSize);
                    Object value = registry.getTypeValue(type, content);
                    logger.trace("    {} {} {}", registry.getTypeName(type), fieldSize, value);
                    record.put(registry.getTypeName(type), value);
                    record.put(NetflowRegistry.TYPEKEY, tpl.type);
                    recordCount++;
                } catch (IndexOutOfBoundsException e) {
                    Throwable t = new IOException(String.format("Reading outside range: %d out of %d", fieldSize, bbuf.readableBytes()), e);
                    record.put(NetflowPacket.EXCEPTION_KEY, t);
                } catch (RuntimeException e) {
                    Throwable t = new IllegalStateException(String.format("Invalid or unhandled Netflow/IPFIX packet: " + Helpers.resolveThrowableException(e)), e);
                    record.put(NetflowPacket.EXCEPTION_KEY, t);
                }
            }
            records.add(record);
        }
        return recordCount;
    }

    @Override
    public Object getId() {
        return sourceId;
    }

    @Override
    public int getLength() {
        return length != -1 ? length : count;
    }

}
