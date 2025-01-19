package loghub.sflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import loghub.sflow.structs.CounterSample;
import loghub.sflow.structs.DynamicStruct;
import loghub.sflow.structs.ExtendedRouter;
import loghub.sflow.structs.ExtendedUser;
import loghub.sflow.structs.FlowSample;
import loghub.sflow.structs.OpaqueStruct;
import loghub.sflow.structs.SampledHeader;
import loghub.sflow.structs.Struct;
import loghub.types.MacAddress;
import loghub.xdr.ReadType;
import loghub.xdr.StructSpecifier;
import loghub.xdr.TypeSpecifier;
import lombok.ToString;

@ToString
public class SflowParser {

    interface StructConstructor {
        Struct get(ByteBuf buf) throws IOException;
    }
    private Map<DataFormat, StructConstructor> registry = new HashMap<>();
    private Map<StructureClass, Map<Integer, Map<Integer, DataFormat>>> structRegistry = new EnumMap<>(StructureClass.class);
    private Map<String, DataFormat> structByName = new HashMap<>();
    public final Set<DataFormat> missing = new HashSet<>();
    private final Map<String, ReadType<?>> byTypeReaders = new HashMap<>();
    private final Map<String, ReadType<?>> byAttributeReaders = new HashMap<>();
    private final Map<String, StructSpecifier> structs = new HashMap<>();

    public SflowParser() {
        getClass().getClassLoader().resources("structs.tsv").forEach(this::loadTsvUrl);
        addConstructor(FlowSample.NAME, b -> new FlowSample(this, b));
        addConstructor(SampledHeader.NAME, b -> new SampledHeader(this, b));
        addConstructor(ExtendedRouter.NAME, b -> new ExtendedRouter(this, b));
        addConstructor(CounterSample.NAME, b -> new CounterSample(this, b));
        addConstructor(ExtendedUser.NAME, b -> new ExtendedUser(this, b));
        byAttributeReaders.put("sampled_ipv4.src_port", ByteBuf::readInt);
        byAttributeReaders.put("sampled_ipv4.protocol", ByteBuf::readInt);
        byAttributeReaders.put("sampled_ipv4.dst_port", ByteBuf::readInt);
        byAttributeReaders.put("sampled_ipv6.src_port", ByteBuf::readInt);
        byAttributeReaders.put("sampled_ipv6.protocol", ByteBuf::readInt);
        byAttributeReaders.put("sampled_ipv6.dst_port", ByteBuf::readInt);
        byAttributeReaders.put("if_counters.ifType", b -> IANAifType.resolve(b.readInt()));
        byAttributeReaders.put("host_descr.uuid", b -> {
            byte[] buffer = new byte[16];
            b.readBytes(buffer);
            return UUID.nameUUIDFromBytes(buffer);
        });
        byTypeReaders.put("mac", this::readMacAddress);
        byTypeReaders.put("ip_v4", this::readIpV4Address);
        byTypeReaders.put("ip_v6", this::readIpV6Address);
        byTypeReaders.put("sflow_data_source", this::readDataSource);
        byTypeReaders.put("charset", this::readCharset);
        byTypeReaders.put("percentage", b -> b.readInt() / 100);
        byTypeReaders.put("utf8string", b -> {
            int size = Math.toIntExact(b.readUnsignedInt());
            byte[] buffer = new byte[size];
            b.readBytes(buffer);
            return new String(buffer, StandardCharsets.UTF_8);
        });
    }

    public void addTypes(Map<String, TypeSpecifier<?>> newTypes) {
        for (Map.Entry<String, TypeSpecifier<?>> e: newTypes.entrySet()) {
            if (! byTypeReaders.containsKey(e.getKey())) {
                byTypeReaders.put(e.getKey(), b -> e.getValue().read(b));
            }
            if (e.getValue() instanceof StructSpecifier && ! structs.containsKey(e.getKey())) {
                structs.put(e.getKey(), (StructSpecifier) e.getValue());
            }
        }
    }

    private void addConstructor(String name, StructConstructor constructor) {
        DataFormat si = structByName.get(name);
        registry.put(si, constructor::get);
    }

    private void loadTsvUrl(URL url) {
        Pattern tabPattern = Pattern.compile("\\t");
        try (InputStream is = url.openStream();
             InputStreamReader isr = new InputStreamReader(is);
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().forEach(line -> {
                String[] descr = tabPattern.split(line);
                if (descr.length >= 4) {
                    StructureClass oclass = StructureClass.resolve(descr[0]);
                    int enterprise = Integer.parseInt(descr[1]);
                    int id = Integer.parseInt(descr[2]);
                    String name = descr[3];
                    DataFormat si = getStructInformation(name, oclass, enterprise, id);
                    structByName.put(name, si);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Struct> T readStruct(StructureClass tclass, ByteBuf data) throws IOException {
        DataFormat df = getStructInformation(tclass, data.readUnsignedInt());
        StructConstructor producer  = registry.get(df);
        if (producer != null) {
            return (T) producer.get(data);
        } else if (structs.containsKey(df.getName())) {
            return (T) new DynamicStruct(df.getName(),this, data);
        } else {
            missing.add(df);
            return (T) new OpaqueStruct(df, data);
        }
    }

    public Map<String, Object> readDynamicStructData(DataFormat df, ByteBuf buf) throws IOException {
        if (structs.containsKey(df.getName())){
            StructSpecifier.CustomReader reader = (n, t,b) -> readStructAttribute(df.getName(), n, t, b);
            return structs.get(df.getName()).read(buf, reader);
        } else {
            return Map.of();
        }
    }

    private Object readStructAttribute(String structName, String attributeName, TypeSpecifier<?> type, ByteBuf buf)
            throws IOException {
        String attributePath = structName + "." + attributeName;
        if (byAttributeReaders.containsKey(attributePath)) {
            return byAttributeReaders.get(attributePath).read(buf);
        } else if (byTypeReaders.containsKey(type.getName())) {
            return byTypeReaders.get(type.getName()).read(buf);
        } else {
            return type.read(buf);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readType(String name, ByteBuf data) throws IOException {
        if (byAttributeReaders.containsKey(name)) {
            return (T) byAttributeReaders.get(name).read(data);
        } else if (byTypeReaders.containsKey(name)){
            return (T) byTypeReaders.get(name).read(data);
        } else {
            return null;
        }
    }

    public DataFormat getStructInformation(String name, StructureClass oclass, int entreprise, int id) {
        return structRegistry.computeIfAbsent(oclass, oc -> new HashMap<>())
                       .computeIfAbsent(entreprise, e -> new HashMap<>())
                       .computeIfAbsent(id, i -> new DataFormat(name, oclass, entreprise, id));
    }

    public DataFormat getStructInformation(StructureClass oclass, long format) {
        int formatId = (int) (format & ((2 << 12) - 1));
        int enterprise = (int) (format >> 12);

        return structRegistry.computeIfAbsent(oclass, oc -> new HashMap<>())
                       .computeIfAbsent(enterprise, e -> new HashMap<>())
                       .computeIfAbsent(formatId, i -> new DataFormat("Unknown", oclass, enterprise, formatId));
    }

    public DataFormat getByName(String name) {
        return structByName.get(name);
    }

    public InetAddress readIpAddress(ByteBuf bbuf) throws IOException {
        int adressType = bbuf.readInt();
        byte[] ipAddrBuffer;

        if (adressType == 1) {
            ipAddrBuffer = new byte[4];
        } else if (adressType == 2) {
            ipAddrBuffer = new byte[16];
        } else {
            throw new IOException("Unknown address type in packet " + adressType);
        }
        bbuf.readBytes(ipAddrBuffer);
        return InetAddress.getByAddress(ipAddrBuffer);
    }

    public InetAddress readIpV4Address(ByteBuf bbuf) throws IOException {
        byte[] ipAddrBuffer = new byte[4];
        bbuf.readBytes(ipAddrBuffer);
        return InetAddress.getByAddress(ipAddrBuffer);
    }

    public InetAddress readIpV6Address(ByteBuf bbuf) throws IOException {
        byte[] ipAddrBuffer = new byte[16];
        bbuf.readBytes(ipAddrBuffer);
        return InetAddress.getByAddress(ipAddrBuffer);
    }

    public MacAddress readMacAddress(ByteBuf buf) {
        buf.readInt();
        byte[] macAddress = new byte[6];
        buf.readBytes(macAddress);
        return new MacAddress((macAddress));
    }

    public Map<String, Object> readDataSource(ByteBuf buf) {
        int sflow_data_source = buf.readInt();
        String type;
        switch (sflow_data_source >>> 23) {
        case 0:
            type = "ifIndex";
            break;
        case 1:
            type = "smonVlanDataSource";
            break;
        case 2:
            type = "entPhysicalEntry";
            break;
        default:
            type = "Unknown(" + (sflow_data_source >>> 3) + ")";
        }
        int index = sflow_data_source & ((2 << 23) - 1);
        Map<String, Object> values = new HashMap<>();
        values.put(type, index);
        return values;
    }

    /**
     * Convert a charset identifier to an effective charset, using <a href="https://www.iana.org/assignments/character-sets/character-sets.xhtml">IANA Character Sets</a>
     * Currently only bother to implement standards one, as defined in {@link StandardCharsets}
     * @param buf
     * @return
     */
    public Charset readCharset(ByteBuf buf) {
        int charset = buf.readInt();
        switch (charset) {
        case 3:
            return StandardCharsets.US_ASCII;
        case 4:
            return StandardCharsets.ISO_8859_1;
        case 106:
            return StandardCharsets.UTF_8;
        case 1013:
            return StandardCharsets.UTF_16BE;
        case 1014:
            return StandardCharsets.UTF_16LE;
        case 1015:
            return StandardCharsets.UTF_16;
        default:
            return Charset.defaultCharset();
        }
    }


    public SFlowDatagram decodePacket(ByteBuf bbuf) throws IOException {
        if (bbuf.readableBytes() < 16) {
            return null;
        }

        SFlowDatagram.SFlowDatagramBuilder builder = SFlowDatagram.builder();
        if (bbuf.readableBytes() < 4) {
            return null;
        }


        int version = bbuf.readInt();
        builder.version(version);
        if (version != 5) { // La version 5 est courante pour SFlow
            throw new IOException("Unhandled sFlow: " + version);
        }

        builder.agent_address(readIpAddress(bbuf));

        builder.sub_agent_id(bbuf.readUnsignedInt());
        builder.sequence_number(bbuf.readUnsignedInt());
        builder.uptime(Duration.ofMillis(bbuf.readUnsignedInt()));

        long numSamples = bbuf.readUnsignedInt();
        List<Struct> samples = new ArrayList<>((int) numSamples);
        builder.samples(samples);
        for (int i = 0; i < numSamples; i++) {
            Struct s = readStruct(StructureClass.SAMPLE_DATA, bbuf);
            samples.add(s);
        }
        // Exemple : Construire un objet représentant le message SFlow
        return builder.build();
    }

}
