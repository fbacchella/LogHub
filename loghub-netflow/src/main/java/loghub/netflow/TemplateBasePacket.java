package loghub.netflow;

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;

public abstract class TemplateBasePacket implements NetflowPacket {

    enum TemplateType {
        Records,
        Options
    }

    private static final Logger logger = LogManager.getLogger();

    protected final Map<TemplateId, Map<Integer, Template>> templates;

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
    private final IpfixInformationElements types;
    private int recordseen = 0;
    private final List<Map<String, Object>> records = new ArrayList<>();

    protected TemplateBasePacket(InetAddress remoteAddr, ByteBuf bbuf, Function<ByteBuf, HeaderInfo> headerreader, IpfixInformationElements types,
            Map<TemplateId, Map<Integer, Template>> templates) {
        this.types = types;
        this.templates = templates;
        short version = bbuf.readShort();
        if (version < 9) {
            throw new RuntimeException("Invalid version");
        }
        header = headerreader.apply(bbuf);
        this.count = header.count;
        this.length = header.length;

        if (count > 0) {
            logger.trace("{} records expected", count);
        }
        exportTime = Instant.ofEpochSecond(Integer.toUnsignedLong(bbuf.readInt()));
        sequenceNumber = Integer.toUnsignedLong(bbuf.readInt());
        sourceId = bbuf.readInt();
        // If lenght is non zero, an Ipfix packet
        if (length > 0) {
            bbuf = bbuf.readBytes(length - 16);
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

    protected void readSet(InetAddress remoteAddr, ByteBuf bbuf, int flowSetCount) {
        int flowSetId = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        logger.trace("set id {}", flowSetId);
        try {
            switch (flowSetId) {
            case 0: // Netflow v9 Template FlowSet
                readTemplateSet(remoteAddr, bbuf.readSlice(length - 4), false);
                break;
            case 1: // Netflow v9 Option Template FlowSet
                readOptionsTemplateNetflowSet(remoteAddr, bbuf.readSlice(length - 4));
                break;
            case 2: // IPFIX Template FlowSet
                readTemplateSet(remoteAddr, bbuf.readSlice(length - 4), true);
                break;
            case 3: // IPFIX Option Template FlowSet
                readOptionsTemplateIpfixSet(remoteAddr, bbuf.readSlice(length - 4));
                break;
            default:
                readDataSet(remoteAddr, bbuf.readSlice(length - 4), flowSetId);
                break;
            }
        } catch (RuntimeException e) {
            logger.error("Failed reading flow set {}, with id {}", flowSetCount, flowSetId);
        }
    }

    private void readDefinition(ByteBuf bbuf, boolean canEntrepriseNumber, Template template, boolean isScope) {
        Number type = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        if ((type.intValue() & 0x8000) != 0 && canEntrepriseNumber) {
            int entrepriseNumber = bbuf.readInt();
            type = ((type.longValue() & ~0x8000) | ((long) entrepriseNumber << 16));
        }
        template.addField(type, length, isScope);
    }

    protected void readTemplateSet(InetAddress remoteAddr, ByteBuf bbuf, boolean canEntrepriseNumber) {
        while (bbuf.isReadable()) {
            recordseen++;
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            logger.trace("  template {}", templateId);
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            if (templateId == 0 && fieldsCount == 0) {
                logger.trace("empty template");
                //It was padding, not a real template template
                break;
            }
            Template template = new Template(TemplateType.Records, fieldsCount);
            for (int i = 0; i < fieldsCount; i++) {
                readDefinition(bbuf, canEntrepriseNumber, template, false);
            }
            templates.computeIfAbsent(new TemplateId(remoteAddr, sourceId), i -> new HashMap<>()).put(templateId, template);
        }
    }

    protected void readOptionsTemplateNetflowSet(InetAddress remoteAddr, ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            logger.trace("  options");
            recordseen++;
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int scopeLength = Short.toUnsignedInt(bbuf.readShort());
            int optionsLength = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template(TemplateType.Options);
            ByteBuf scopes = bbuf.readSlice(scopeLength);
            ByteBuf options = bbuf.readSlice(optionsLength);
            // The test ensure there is more than padding left in the ByteBuf
            while (scopes.isReadable(3)) {
                readDefinition(scopes, false, template, true);
            }
            // The test ensure there is more than padding left in the ByteBuf
            while (options.isReadable(3)) {
                readDefinition(options, false, template, false);
            }
            templates.computeIfAbsent(new TemplateId(remoteAddr, sourceId), i -> new HashMap<>()).put(templateId, template);
        }
    }

    private void readOptionsTemplateIpfixSet(InetAddress remoteAddr, ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            logger.trace("  options");
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            int scopesCount = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template(TemplateType.Options, fieldsCount);
            for (int i = 0; i < scopesCount; i++) {
                readDefinition(bbuf, true, template, true);
            }
            for (int i = scopesCount; i < fieldsCount; i++) {
                readDefinition(bbuf, true, template, false);
            }
            templates.computeIfAbsent(new TemplateId(remoteAddr, sourceId), i -> new HashMap<>()).put(templateId, template);
        }
    }

    protected void readDataSet(InetAddress remoteAddr, ByteBuf bbuf, int flowSetId) {
        TemplateId key = new TemplateId(remoteAddr, sourceId);
        if (! templates.containsKey(key)) {
            return;
        }
        Template tpl = templates.get(key).get(flowSetId);
        if (tpl == null) {
            return;
        }
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            recordseen++;
            Map<String, Object> record = new HashMap<>(tpl.getSizes());
            logger.trace("  data");
            for (int i = 0; i < tpl.getSizes(); i++) {
                Number type;
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
                    Object value = types.getValue(type.intValue(), content);
                    logger.trace("    {} {} {}", types.getName(type.intValue()), fieldSize, value);
                    record.put(types.getName(type.intValue()), value);
                    record.put(PacketFactory.TYPEKEY, tpl.type);
                } catch (IndexOutOfBoundsException e) {
                    throw new IllegalStateException(String.format("Reading outside range: %d out of %d", fieldSize, bbuf.readableBytes()));
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
