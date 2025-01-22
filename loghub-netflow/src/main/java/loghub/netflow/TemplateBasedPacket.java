package loghub.netflow;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
    protected final Instant exportTime;
    protected final long sequenceNumber;
    protected final int length;
    protected final int count;
    private int recordseen = 0;
    private final List<Map<String, Object>> records = new ArrayList<>();
    private final NetflowRegistry registry;
    @Getter
    private boolean withFailure = false;

    protected TemplateBasedPacket(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry) {
        this.registry = registry;
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
        int flowSetCount = 0;
        while (bbuf.isReadable()) {
            readSet(remoteAddr, bbuf, ++flowSetCount);
        }
        if (count > 0 && recordseen > count) {
            logger.debug("too much records seen: {}/{}", recordseen, count);
        } else if (recordseen < count) {
            logger.debug("not enough records: {}/{}", recordseen, count);
        }
    }

    protected abstract HeaderInfo readHeader(ByteBuf bbuf);

    protected void readSet(InetAddress remoteAddr, ByteBuf bbuf, int flowSetCount) {
        Template.TemplateId key = new Template.TemplateId(remoteAddr, sourceId);
        int flowSetId = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        logger.trace("set id {}", flowSetId);
        try {
            switch (flowSetId) {
            case 0: // Netflow v9 Template FlowSet
                recordseen += registry.readTemplateSet(key, bbuf.readSlice(length - 4), false);
                break;
            case 1: // Netflow v9 Option Template FlowSet
                recordseen += registry.readOptionsTemplateNetflowSet(key, bbuf.readSlice(length - 4));
                break;
            case 2: // IPFIX Template FlowSet
                recordseen += registry.readTemplateSet(key, bbuf.readSlice(length - 4), true);
                break;
            case 3: // IPFIX Option Template FlowSet
                recordseen += registry.readOptionsTemplateIpfixSet(key, bbuf.readSlice(length - 4));
                break;
            default:
                readDataSet(key, bbuf.readSlice(length - 4), flowSetId);
                break;
            }
        } catch (RuntimeException e) {
            withFailure = true;
            logger.atError().withThrowable(logger.isDebugEnabled() ? e : null).log("Failed reading flow set {}, with id {}", flowSetCount, flowSetId);
        }
    }

    protected void readDataSet(Template.TemplateId key, ByteBuf bbuf, int flowSetId) {
        registry.getTemplate(key, flowSetId).ifPresent(t -> readDataSet(t, bbuf));
    }

    private void readDataSet(Template tpl, ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(4)) {
            recordseen++;
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
    }

    @Override
    public Object getId() {
        return sourceId;
    }

    @Override
    public Instant getExportTime() {
        return exportTime;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public int getLength() {
        return length != -1 ? length : count;
    }

    @Override
    public List<Map<String, Object>> getRecords() {
        return Collections.unmodifiableList(records);
    }

}
