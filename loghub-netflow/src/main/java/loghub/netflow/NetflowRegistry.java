package loghub.netflow;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;

public class NetflowRegistry {

    public static final String TYPEKEY = "__type";
    private final Map<Template.TemplateId, Map<Integer, Template>> templates = new ConcurrentHashMap<>();
    private final IpfixInformationElements ipfixtypes;

    public NetflowRegistry() {
        ipfixtypes = new IpfixInformationElements();
    }

    public NetflowPacket parsePacket(InetAddress remoteAddr, ByteBuf bbuf) throws IOException {
        bbuf.markReaderIndex();
        short version = bbuf.readShort();
        bbuf.resetReaderIndex();
        switch (version) {
        case 5:
            return new Netflow5Packet(bbuf);
        case 9:
            return new Netflow9Packet(remoteAddr, bbuf, this);
        case 10:
            return new IpfixPacket(remoteAddr, bbuf, this);
        default:
            throw new IOException("Unsupported netflow/IPFIX packet version: " + version);
        }
    }

    private void readDefinition(ByteBuf bbuf, boolean canEntrepriseNumber, Template template, boolean isScope) {
        int type = Short.toUnsignedInt(bbuf.readShort());
        int length = Short.toUnsignedInt(bbuf.readShort());
        if ((type & 0x8000) != 0 && canEntrepriseNumber) {
            int entrepriseNumber = bbuf.readInt();
            type = ((type & ~0x8000) | (entrepriseNumber << 16));
        }
        template.addField(type, length, isScope);
    }

    void readTemplateSet(Template.TemplateId key, ByteBuf bbuf, boolean canEntrepriseNumber) {
        while (bbuf.isReadable()) {
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            if (templateId == 0 && fieldsCount == 0) {
                //It was padding, not a real template template
                break;
            }
            Template template = new Template(Template.TemplateType.RECORDS, fieldsCount);
            for (int i = 0; i < fieldsCount; i++) {
                readDefinition(bbuf, canEntrepriseNumber, template, false);
            }
            templates.computeIfAbsent(key, i -> new HashMap<>()).put(templateId, template);
        }
    }

    void readOptionsTemplateNetflowSet(Template.TemplateId key, ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(4)) {
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int scopeLength = Short.toUnsignedInt(bbuf.readShort());
            int optionsLength = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template(Template.TemplateType.OPTIONS);
            ByteBuf scopes = bbuf.readSlice(scopeLength);
            ByteBuf options = bbuf.readSlice(optionsLength);
            // The test ensure there is more than padding left in the ByteBuf
            while (scopes.isReadable(4)) {
                readDefinition(scopes, false, template, true);
            }
            // The test ensure there is more than padding left in the ByteBuf
            while (options.isReadable(4)) {
                readDefinition(options, false, template, false);
            }
            templates.computeIfAbsent(key, i -> new HashMap<>()).put(templateId, template);
        }
    }

    void readOptionsTemplateIpfixSet(Template.TemplateId key, ByteBuf bbuf) {
        // The test ensure there is more than padding left in the ByteBuf
        while (bbuf.isReadable(3)) {
            int templateId = Short.toUnsignedInt(bbuf.readShort());
            int fieldsCount = Short.toUnsignedInt(bbuf.readShort());
            int scopesCount = Short.toUnsignedInt(bbuf.readShort());
            Template template = new Template(Template.TemplateType.OPTIONS, fieldsCount);
            for (int i = 0; i < scopesCount; i++) {
                readDefinition(bbuf, true, template, true);
            }
            for (int i = scopesCount; i < fieldsCount; i++) {
                readDefinition(bbuf, true, template, false);
            }
            templates.computeIfAbsent(key, i -> new HashMap<>()).put(templateId, template);
        }
    }

    Optional<Template> getTemplate(Template.TemplateId key, int flowSetId) {
        if (!templates.containsKey(key)) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(templates.get(key).get(flowSetId));
        }
    }

    public String getTypeName(int i) {
        // Not an enterprise type
        if (i <= 65535) {
            return ipfixtypes.getName(i);
        } else {
            int enterprise = i >>> 16;
            int sybType = i & 65535;
            return "enterprise_" + enterprise + "_" + sybType;
        }
    }

    public Object getTypeValue(int i, ByteBuf bbuf) {
        return ipfixtypes.getValue(i, bbuf);
    }
}
