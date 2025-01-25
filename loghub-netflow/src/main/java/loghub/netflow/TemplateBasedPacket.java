package loghub.netflow;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
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
    private Instant systemInitTime = null;

    protected TemplateBasedPacket(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry) throws IOException {
        short version = bbuf.readShort();
        if (version < 9) {
            throw new IllegalArgumentException("Invalid version " + version);
        }
        header = readHeader(bbuf);
        this.count = header.count;
        this.length = header.length;

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
                throw new IOException("Failed reading flow set", e);
            }
        }
        records = List.copyOf(tmpRecords);
        if (count > 0 && flowSeen > count) {
            logger.debug("Too much records seen: {}/{}/{}", flowSeen, flowSetSeen, count);
        } else if (flowSeen < count) {
            logger.debug("Not enough records: {}/{}/{}", flowSeen, flowSetSeen, count);
        }
    }

    protected abstract HeaderInfo readHeader(ByteBuf bbuf);

    protected int readSet(InetAddress remoteAddr, ByteBuf bbuf, NetflowRegistry registry, List<Map<String, Object>> records) {
        Template.TemplateId key = new Template.TemplateId(remoteAddr, sourceId);
        int flowSetId = Short.toUnsignedInt(bbuf.readShort());
        int setLength = Short.toUnsignedInt(bbuf.readShort());
        switch (flowSetId) {
        case 0: // Netflow v9 Template FlowSet
            registry.readTemplateSet(key, bbuf.readSlice(setLength - 4), false);
            return 1;
        case 1: // Netflow v9 Option Template FlowSet
            registry.readOptionsTemplateNetflowSet(key, bbuf.readSlice(setLength - 4));
            return 1;
        case 2:
            registry.readTemplateSet(key, bbuf.readSlice(setLength - 4), true);// IPFIX Template FlowSet
            return 1;
        case 3: // IPFIX Option Template FlowSet
            registry.readOptionsTemplateIpfixSet(key, bbuf.readSlice(setLength - 4));
            return 1;
        default:
            return readDataSet(key, bbuf.readSlice(setLength - 4), flowSetId, registry, records);
        }
    }

    protected int readDataSet(Template.TemplateId key, ByteBuf bbuf, int flowSetId, NetflowRegistry registry, List<Map<String, Object>> records) {
        return registry.getTemplate(key, flowSetId)
                       .stream()
                       .mapToInt(t -> readDataSet(t, bbuf, registry, records))
                       .sum();
    }

    private int readDataSet(Template tpl, ByteBuf bbuf, NetflowRegistry registry, List<Map<String, Object>> records) {
        int flowSeen = 0;

        Map<String, Object> scopes;
        if (tpl.getScopeCount() > 0) {
            scopes = new HashMap<>();
        } else {
            scopes = Map.of();
        }

        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(4)) {
            Map<String, Object> recordData = new HashMap<>(tpl.getSizes());
            recordData.put(NetflowRegistry.TYPEKEY, tpl.type);
            for (int i = 0; i < tpl.getSizes(); i++) {
                int fieldSize = tpl.getSize(i);
                try {
                    if (fieldSize == 65535) {
                        fieldSize = Byte.toUnsignedInt(bbuf.readByte());
                        if (fieldSize == 255) {
                            fieldSize = Short.toUnsignedInt(bbuf.readShort());
                        }
                    }
                    ByteBuf content = bbuf.readSlice(fieldSize);
                    int type = tpl.types.get(i);
                    Object value = registry.getTypeValue(type, content);
                    String typeName = (tpl.isScope(i) && getVersion() == 9) ? Template.resolveScope(type) : registry.getTypeName(type);
                    if ("paddingOctets".equals(typeName)) {
                        continue;
                    }
                    if (tpl.isScope(i)) {
                        scopes.put(typeName, value);
                    } else {
                        recordData.put(typeName, value);
                    }
                } catch (IndexOutOfBoundsException e) {
                    Throwable t = new IOException(String.format("Reading outside range: %d out of %d", fieldSize, bbuf.readableBytes()), e);
                    recordData.put(NetflowPacket.EXCEPTION_KEY, t);
                } catch (RuntimeException e) {
                    Throwable t = new IllegalStateException(String.format("Invalid or unhandled Netflow/IPFIX packet: %s", Helpers.resolveThrowableException(e)), e);
                    recordData.put(NetflowPacket.EXCEPTION_KEY, t);
                }
            }
            if (! scopes.isEmpty()) {
                recordData.put("scope", scopes);
            }
            if (recordData.containsKey("systemInitTimeMilliseconds")) {
                systemInitTime = (Instant) recordData.get("systemInitTimeMilliseconds");
            }
            if (systemInitTime == null && header.sysUpTime != 0) {
                systemInitTime = exportTime.minusMillis(header.sysUpTime);
            }
            if (systemInitTime != null) {
                recordData.put("__systemInitTime", systemInitTime);
            }
            Duration endRelative = null;
            if (recordData.containsKey("flowEndSysUpTime")) {
                endRelative = Duration.ofMillis(((Number) recordData.get("flowEndSysUpTime")).longValue());
                if (systemInitTime != null) {
                    Temporal endTime = endRelative.addTo(systemInitTime);
                    recordData.put("__endTime", endTime);
                }
            }
            Duration startRelative = null;
            if (recordData.containsKey("flowStartSysUpTime")) {
                startRelative = Duration.ofMillis(((Number) recordData.get("flowStartSysUpTime")).longValue());
                if (systemInitTime != null) {
                    Temporal startTime = startRelative.addTo(systemInitTime);
                    recordData.put("__startTime", startTime);
                }
            }
            if (endRelative != null && startRelative != null) {
                recordData.put("__duration", endRelative.minus(startRelative));
            }
            flowSeen++;
            records.add(recordData);
        }
        return flowSeen;
    }

    @Override
    public Object getId() {
        return sourceId;
    }

}
