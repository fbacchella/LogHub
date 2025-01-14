package loghub.sflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import loghub.sflow.structs.CounterSample;
import loghub.sflow.structs.EthernetCounters;
import loghub.sflow.structs.ExtendedRouter;
import loghub.sflow.structs.ExtendedSwitch;
import loghub.sflow.structs.FlowSample;
import loghub.sflow.structs.IfCounters;
import loghub.sflow.structs.LagPortStats;
import loghub.sflow.structs.OpaqueStruct;
import loghub.sflow.structs.SampledHeader;
import loghub.sflow.structs.Struct;
import lombok.ToString;

@ToString
public class SflowParser {
    private Map<DataFormat, Function<ByteBuf, ? extends Struct>> registry = new HashMap<>();
    private Map<StructureClass, Map<Integer, Map<Integer, DataFormat>>> structRegistry = new EnumMap<>(StructureClass.class);
    private Map<String, DataFormat> structByName = new HashMap<>();
    public final Set<DataFormat> missing = new HashSet<>();

    public SflowParser() {
        getClass().getClassLoader().resources("structs.tsv").forEach(this::loadTsvUrl);
        addConstructor(FlowSample.NAME, FlowSample::new);
        addConstructor(SampledHeader.NAME, SampledHeader::new);
        addConstructor(ExtendedRouter.NAME, ExtendedRouter::new);
        addConstructor(ExtendedSwitch.NAME, ExtendedSwitch::new);
        addConstructor(CounterSample.NAME, CounterSample::new);
        addConstructor(LagPortStats.NAME, LagPortStats::new);
        addConstructor(EthernetCounters.NAME, EthernetCounters::new);
        addConstructor(IfCounters.NAME, IfCounters::new);
    }

    private void addConstructor(String name, BiFunction<SflowParser, ByteBuf, ? extends Struct> constructor) {
        DataFormat si = structByName.get(name);
        registry.put(si, b -> constructor.apply(this, b));
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
    public <T extends Struct> T readStruct(StructureClass tclass, ByteBuf data) {
        DataFormat df = getStructInformation(tclass, data.readUnsignedInt());
        Function<ByteBuf, ? extends Struct> producer  = registry.get(df);
        if (producer != null) {
            return (T) producer.apply(data);
        } else {
            missing.add(df);
            return (T) new OpaqueStruct(df, data);
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

        builder.agentAddress(readIpAddress(bbuf));

        builder.subAgentId(bbuf.readUnsignedInt());
        builder.sequenceNumber(bbuf.readUnsignedInt());
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
