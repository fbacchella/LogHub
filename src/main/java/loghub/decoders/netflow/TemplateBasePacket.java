package loghub.decoders.netflow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public abstract class TemplateBasePacket implements NetflowPacket {

    protected final Map<Integer, Map<Integer,Template>> templates = new HashMap<>();

    private static class Template {
        private final List<Number> types;
        private final List<Integer> sizes;
        private final List<Boolean> areScops;
        private Template(int count) {
            types = new ArrayList<>(count);
            sizes = new ArrayList<>(count);
            areScops = new ArrayList<>(count);
        }
        private Template() {
            types = new ArrayList<>();
            sizes = new ArrayList<>();
            areScops = new ArrayList<>();
        }
        private void addField(Number type, int size, boolean isScope) {
            types.add(type);
            sizes.add(size);
            areScops.add(isScope);
        }
        private int getSizes() {
            return sizes.size();
        }
        private int getSize(int record) {
            return sizes.get(record);
        }
        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            for(int i = 0 ; i < types.size() ; i++) {
                buffer.append(String.format("%d[%d]%s, ", types.get(i), sizes.get(i), areScops.get(i) ? "S" : ""));
            }
            buffer.delete(buffer.length() - 2 , buffer.length());
            return buffer.toString();
        }
    }
    
    protected static class HeaderInfo {
        int count = -1;
        int length = -1;
        long sysUpTime = 0;
    }

    protected final int id;
    protected final Instant exportTime;
    protected final long sequenceNumber;
    protected final int length;
    protected final int count;
    private int recordseen = 0;

    protected TemplateBasePacket(ByteBuf bbuf, Function<ByteBuf, HeaderInfo> headerreader) {
        //Skip version
        short version = bbuf.readShort();
        if (version < 9) {
            throw new RuntimeException("Invalid version");
        }
        HeaderInfo header = headerreader.apply(bbuf);
        this.count = header.count;
        this.length = header.length;
        
        System.out.format("%d records\n", count);
        exportTime = Instant.ofEpochSecond(Integer.toUnsignedLong(bbuf.readInt()));
        sequenceNumber = Integer.toUnsignedLong(bbuf.readInt());
        id = bbuf.readInt();
        // If lenght is non zero, an Ipfix packet
        if (length > 0) {
            bbuf = bbuf.readBytes(length - 16);
        }
        while(bbuf.isReadable()) {
            readSet(bbuf);
            if (count > 0 && recordseen > count) {
                System.out.println("too much records");
                //break;
            }
        }
    }

    protected void readSet(ByteBuf bbuf) {
        try {
            int flowSetId = Short.toUnsignedInt(bbuf.readShort());
            int length = Short.toUnsignedInt(bbuf.readShort());
            System.out.format("reading set %d\n", flowSetId);
            switch (flowSetId) {
            case 0: // Netflow v9 Template FlowSet
                readTemplateSet(bbuf.readSlice(length - 4), false);
                break;
            case 1: // Netflow v9 Option Template FlowSet
                readOptionsTemplateNetflowSet(bbuf.readSlice(length - 4));
                break;
            case 2: // IPFIX Template FlowSet
                readTemplateSet(bbuf.readSlice(length - 4), true);
                break;
            case 3: // IPFIX Option Template FlowSet
                readOptionsTemplateIpfixSet(bbuf.readSlice(length - 4));
                break;
            default:
                readDataSet(bbuf.readSlice(length - 4), flowSetId);
                break;
            }
        } catch (Exception e) {
            System.out.println("******failed********");
        }
    }

    private void readDefinition(ByteBuf bbuf, boolean canEntrepriseNumber, Template template, boolean isScope) {
        Number type = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        if ((type.intValue() & 0x8000) != 0 && canEntrepriseNumber) {
            int entrepriseNumber = bbuf.readInt();
            type = ((type.longValue() & ~0x8000) | (entrepriseNumber << 16));
        }
        template.addField(type, length, isScope);
    }

    protected void readTemplateSet(ByteBuf bbuf, boolean canEntrepriseNumber) {
        while(bbuf.isReadable()) {
            System.out.println("  template");
            recordseen++;
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            if (templateId == 0 && fieldsCount == 0) {
                //It was padding, not a real template template
                break;
            }
            Template template = new Template(fieldsCount);
            for (int i = 0 ; i < fieldsCount ; i++) {
                readDefinition(bbuf, canEntrepriseNumber, template, false);
            }
            templates.computeIfAbsent(id, i -> new HashMap<>()).put(templateId, template);
        }
    }

    protected void readOptionsTemplateNetflowSet(ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            System.out.println("  options");
            recordseen++;
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int scopeLength = Short.toUnsignedInt(bbuf.readShort());
            int optionsLength = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template();
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
            templates.computeIfAbsent(id, i -> new HashMap<>()).put(templateId, template);
        }
    }


    private void readOptionsTemplateIpfixSet(ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            System.out.println("  options");
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            int scopesCount = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template(fieldsCount);
            for(int i = 0; i < scopesCount; i++) {
                readDefinition(bbuf, true, template, true);
            }
            for(int i = scopesCount; i < fieldsCount; i++) {
                readDefinition(bbuf, true, template, false);
            } 
            templates.computeIfAbsent(id, i -> new HashMap<>()).put(templateId, template);
        }
    }

    protected void readDataSet(ByteBuf bbuf, int flowSetId) {
        //System.out.println(templates);
        //System.out.format("%d -> %s\n", id, templates.get(id));
        //System.out.format("%d -> %d -> %s\n", id, flowSetId, templates.get(this.id).get(flowSetId));
        if ( ! templates.containsKey(id)) {
            return;
        }
        Template tpl = templates.get(id).get(flowSetId);
        if (tpl == null) {
            return;
        }
//        System.out.println(templates);
//        System.out.println(tpl);
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            recordseen++;
            System.out.println("  data");
            for (int i = 0 ; i < tpl.getSizes() ; i++) {
                int fieldSize = tpl.getSize(i);
                //System.out.format("%d %d\n", i, fieldSize);
                if (fieldSize == 65535) {
                    fieldSize = Byte.toUnsignedInt(bbuf.readByte());
                    if (fieldSize == 255) {
                        fieldSize = Short.toUnsignedInt(bbuf.readShort());
                    }
                }
                bbuf.skipBytes(fieldSize);
            }
        }
    }


    @Override
    public Object getId() {
        return id;
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
        return Collections.emptyList();
    }

}
