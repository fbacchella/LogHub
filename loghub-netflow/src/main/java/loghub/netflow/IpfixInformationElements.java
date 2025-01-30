package loghub.netflow;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import loghub.jackson.JacksonBuilder;
import loghub.types.MacAddress;
import lombok.Data;

class IpfixInformationElements {

    @Data
    @JsonPropertyOrder({"elementId", "name", "type", "semantics", "status", "description",
                        "units", "range", "additional", "references", "revision", "date"})
    public static class Element {
        public final int elementId;
        public final String name;
        public final String type;
        public final String semantics;
        public final String status;
        public final String description;
        public final String units;
        public final String range;
        public final String additional;
        public final String references;
        public final String revision;
        public final String date;

        public Element() {
            elementId = 0;
            this.name = "";
            this.type = "";
            this.semantics = "";
            this.status = "";
            this.description = "";
            this.units = "";
            this.range = "";
            this.additional = "";
            this.references = "";
            this.revision = "";
            this.date = "";
        }

    }

    private static final Logger logger = LogManager.getLogger();
    private static final ThreadLocal<byte[]> buffer4 = ThreadLocal.withInitial(() -> new byte[4]);
    private static final ThreadLocal<byte[]> buffer6 = ThreadLocal.withInitial(() -> new byte[6]);
    private static final ThreadLocal<byte[]> buffer8 = ThreadLocal.withInitial(() -> new byte[8]);
    private static final ThreadLocal<byte[]> buffer16 = ThreadLocal.withInitial(() -> new byte[16]);

    // Downloaded from https://www.iana.org/assignments/ipfix/ipfix-information-elements.csv
    private static final String CSVSOURCE = "ipfix-information-elements.csv";

    public final Map<Integer, Element> elements;

    public IpfixInformationElements() {
        CsvMapper mapper = JacksonBuilder.get(CsvMapper.class).getMapper();
        CsvSchema elementSchema = mapper.schemaFor(Element.class).withHeader();
        ObjectReader csvReader = mapper.readerFor(Element.class).with(elementSchema);
        try (Reader in = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(CSVSOURCE), StandardCharsets.US_ASCII)) {
            Map<Integer, Element> buildElements = new HashMap<>();
            MappingIterator<Element> i = csvReader.readValues(in);
            while (i.hasNextValue()) {
                try {
                    Element e = i.nextValue();
                    buildElements.put(e.elementId, e);
                } catch (JsonMappingException ex) {
                    // some broken lines in ipfix-information-elements.csv to ignore
                    logger.debug("Ignored not CVS line in ipfix-information-elements.csv: {}",  ex.getMessage());
                }
            }
            elements = Collections.unmodifiableMap(buildElements);
        } catch (IOException ex) {
            throw new IllegalStateException("Can't use ipfix-information-elements.csv", ex);
        }
    }

    public String getName(int i) {
        return elements.containsKey(i) ? elements.get(i).name : Integer.toString(i);
    }

    public Object getValue(int i, ByteBuf bbuf) {
        Element e = elements.get(i);
        if (e == null) {
            return readByteArray(bbuf);
         } else {
            return evalByName(e, bbuf)
                    .or(() -> evalByType(e, bbuf))
                    .or(() -> evalByAdHoc(e, bbuf))
                    .orElseGet(() -> readByteArray(bbuf));
        }
    }

    private byte[] readByteArray(ByteBuf bbuf) {
        if (bbuf.isReadable()) {
            byte[] buffer = new byte[bbuf.readableBytes()];
            bbuf.readBytes(buffer);
            return buffer;
        } else {
            return new byte[0];
        }
    }

    private Optional<Object> evalByName(Element e, ByteBuf bbuf) {
        switch (e.name) {
        case "applicationId":
            return Optional.of(decodeApplicationId(bbuf));
        case "icmpTypeCodeIPv4":
            if (bbuf.isReadable(2)) {
                byte type = bbuf.readByte();
                byte code = bbuf.readByte();
                return Optional.of(Map.of("type", type, "code", code));
            } else {
                return Optional.empty();
            }
        case "samplingAlgorithm":
            if (bbuf.isReadable()) {
                byte value = bbuf.readByte();
                switch (value) {
                case 1:
                    return Optional.of("Deterministic Sampling");
                case 2:
                    return Optional.of("Random Sampling");
                default:
                    return Optional.of(value);
                }
            } else {
                return Optional.empty();
            }
        case "forwardingStatus":
            if (bbuf.isReadable()) {
                return Optional.of(decodeForwardingStatus(bbuf));
            } else {
                return Optional.empty();
            }
        case "Reserved":
            return Optional.of(readByteArray(bbuf));
        case "flowEndReason":
            if (bbuf.isReadable()) {
                byte value = bbuf.readByte();
                switch (value) {
                case 0: return Optional.of("reserved");
                case 1: return Optional.of("idle timeout");
                case 2: return Optional.of("active timeout");
                case 3: return Optional.of("end of Flow detected");
                case 4: return Optional.of("forced end");
                case 5: return Optional.of("lack of resources");
                default: return Optional.of("Unassigned: " + value);
                }
            } else {
                return Optional.empty();
            }
        case "ipClassOfService":
            if (bbuf.isReadable()) {
                return Optional.of(decodeDscp(bbuf));
            } else {
                return Optional.empty();
            }
        default:
            return Optional.empty();
        }
    }

    private Map<String, Object> decodeForwardingStatus(ByteBuf bbuf) {
        short value = bbuf.readUnsignedByte();
        String status;
        String reason;
        switch (value) {
        case 0x040:
            status = "Forwarded";
            reason = "Unknown";
            break;
        case 0x41:
            status = "Forwarded";
            reason = "Fragmented";
            break;
        case 0x42:
            status = "Forwarded";
            reason = "Not Fragmented";
            break;
        case 0x43:
            status = "Forwarded";
            reason = "Tunneled";
            break;
        case 0x44:
            status = "Forwarded";
            reason = "ACL Redirect";
            break;
        case 0x80:
            status = "Dropped";
            reason = "Unknown";
            break;
        case 0x81:
            status = "Dropped";
            reason = "ACL deny";
            break;
        case 0x82:
            status = "Dropped";
            reason = "ACL drop";
            break;
        case 0x83:
            status = "Dropped";
            reason = "Unroutable";
            break;
        case 0x84:
            status = "Dropped";
            reason = "Adjacency";
            break;
        case 0x85:
            status = "Dropped";
            reason = "Fragmentation and DF set";
            break;
        case 0x86:
            status = "Dropped";
            reason = "Bad header checksum";
            break;
        case 0x87:
            status = "Dropped";
            reason = "Bad total Length";
            break;
        case 0x88:
            status = "Dropped";
            reason = "Bad header length";
            break;
        case 0x89:
            status = "Dropped";
            reason = "Bad TTL";
            break;
        case 0x8a:
            status = "Dropped";
            reason = "Policer";
            break;
        case 0x8b:
            status = "Dropped";
            reason = "WRED";
            break;
        case 0x8c:
            status = "Dropped";
            reason = "RPF";
            break;
        case 0x8d:
            status = "Dropped";
            reason = "For us";
            break;
        case 0x8e:
            status = "Dropped";
            reason = "Bad output interface";
            break;
        case 0x8f:
            status = "Dropped";
            reason = "Hardware";
            break;
        case 0xc0:
            status = "Consumed";
            reason = "Unknown";
            break;
        case 0xc1:
            status = "Consumed";
            reason = "Punt Adjacency";
            break;
        case 0xc2:
            status = "Consumed";
            reason = "Incomplete Adjacency";
            break;
        case 0xc3:
            status = "Consumed";
            reason = "For us";
            break;
        default:
            switch ((byte) (value >>> 6)) {
            case 0:
                status = "Unknown";
                break;
            case 1:
                status = "Forwarded";
                break;
            case 2:
                status = "Dropped";
                break;
            case 3:
                status = "Consumed";
                break;
            default:
                // Not reachable
                throw new IllegalStateException();
            }
            reason = "Unassigned: " + (value & (2^6 -1));
        }
        return Map.of("reason", reason, "status", status);
    }

    private Map<String, Object> decodeDscp(ByteBuf bbuf) {
        int value = Byte.toUnsignedInt(bbuf.readByte());
        int ecn = value & 3;
        int dscp = value >>> 2;
        int pool;
        int codepoint;
        if ((dscp & 1) == 0) {
            pool = 1;
            codepoint = dscp >>> 1;
        } else if ((dscp & 3) == 3) {
            pool = 2;
            codepoint = dscp >>> 2;
        } else {
            pool = 3;
            codepoint = dscp >>> 2;
        }
        String dscpName;
        String serviceClass;
        Map<String, Object> dscpMap = new HashMap<>();
        dscpMap.put("pool", pool);
        dscpMap.put("codePoint", codepoint);
        switch (dscp) {
        case 0:
            dscpName = "DF";
            serviceClass = "Standard";
            break;
        case 1:
            dscpName = "LE";
            serviceClass = "Lower-effort";
            break;
        case 10:
            dscpName = "AF11";
            serviceClass = "High-throughput data";
            break;
        case 12:
            dscpName = "AF12";
            serviceClass = "High-throughput data";
            break;
        case 14:
            dscpName = "AF13";
            serviceClass = "High-throughput data";
            break;
        case 16:
            dscpName = "CS2";
            serviceClass = "OAM";
            break;
        case 18:
            dscpName = "AF21";
            serviceClass = "Low-latency data";
            break;
        case 20:
            dscpName = "AF22";
            serviceClass = "Low-latency data";
            break;
        case 22:
            dscpName = "AF23";
            serviceClass = "Low-latency data";
            break;
        case 24:
            dscpName = "CS3";
            serviceClass = "Broadcast video";
            break;
        case 26:
            dscpName = "AF31";
            serviceClass = "Multimedia streaming";
            break;
        case 28:
            dscpName = "AF32";
            serviceClass = "Multimedia streaming";
            break;
        case 30:
            dscpName = "AF33";
            serviceClass = "Multimedia streaming";
            break;
        case 32:
            dscpName = "CS4";
            serviceClass = "Real-time interactive";
            break;
        case 34:
            dscpName = "AF41";
            serviceClass = "Multimedia conferencing";
            break;
        case 36:
            dscpName = "AF42";
            serviceClass = "Multimedia conferencing";
            break;
        case 38:
            dscpName = "AF43";
            serviceClass = "Multimedia conferencing";
            break;
        case 40:
            dscpName = "CS5";
            serviceClass = "Signaling";
            break;
        case 46:
            dscpName = "EF";
            serviceClass = "Telephony";
            break;
        case 48:
            dscpName = "CS6";
            serviceClass = "Network control";
            break;
        case 56:
            dscpName = "CS7";
            serviceClass = "Reserved for future use";
            break;
        default:
            dscpName = "";
            serviceClass = "";
        }
        if (! dscpName.isEmpty()) {
            dscpMap.put("dscpName", dscpName);
            dscpMap.put("serviceClass", serviceClass);
        }
        String ecnKeyword;
        switch (ecn) {
        case 0:
            ecnKeyword = "Not-ECT";
            break;
        case 1:
            ecnKeyword = "ECT(1)";
            break;
        case 2:
            ecnKeyword = "ECT(0)";
            break;
        case 3:
            ecnKeyword = "CE";
            break;
        default:
            // Never reached
            ecnKeyword = "";
        }
        return Map.of("ECN", ecnKeyword, "DSCP", dscpMap);
    }

    private Optional<Object> evalByType(Element e, ByteBuf bbuf) {
        switch (e.type) {
        case "boolean": {
            if (bbuf.isReadable(1)) {
                byte value = bbuf.readByte();
                return Optional.of(value == 1);
            } else {
                return Optional.empty();
            }
        }
        case "octetArray":
            return Optional.of(readByteArray(bbuf));
        case "dateTimeSeconds": {
            long value = readNumValue(bbuf);
            return Optional.of(Instant.ofEpochSecond(value));
        }
        case "dateTimeMilliseconds": {
            long value = readNumValue(bbuf);
            return Optional.of(Instant.ofEpochMilli(value));
        }
        case "dateTimeMicroseconds": {
            long value = readNumValue(bbuf);
            return Optional.of(Instant.ofEpochSecond(0, value * 1000));
        }
        case "dateTimeNanoseconds": {
            long value = readNumValue(bbuf);
            return Optional.of(Instant.ofEpochSecond(0, value));
        }
        case "float32": {
            if (bbuf.isReadable(4)) {
                return Optional.of(bbuf.readFloat());
            } else {
                return Optional.empty();
            }
        }
        case "float64": {
            if (bbuf.isReadable(8)) {
                return Optional.of(bbuf.readDouble());
            } else {
                return Optional.empty();
            }
        }
        case "macAddress": {
            byte[] macBuffer;
            if (bbuf.isReadable(6)) {
                macBuffer = buffer6.get();
            } else if (bbuf.isReadable(8)) {
                macBuffer = buffer8.get();
            } else {
                return Optional.empty();
            }
            bbuf.readBytes(macBuffer);
            return Optional.of(new MacAddress(macBuffer));
        }
        case "string": {
            if (bbuf.isReadable()) {
                return Optional.of(readString(bbuf));
            } else {
                return Optional.empty();
            }
        }
        case "ipv4Address":
            return readIpAddress(4, bbuf);
        case "ipv6Address":
            return readIpAddress(16, bbuf);
        default:
            return Optional.empty();
        }
    }

    private String readString(ByteBuf bbuf) {
        // Strings are often padded with \0 so cut it
        byte[] stringBuffer = new byte[bbuf.readableBytes()];
        bbuf.readBytes(stringBuffer);
        int zoffest = stringBuffer.length;
        for (int p = 0; p < stringBuffer.length; p++) {
            if (stringBuffer[p] == 0) {
                zoffest = p;
                break;
            }
        }
        return new String(stringBuffer, 0, zoffest, CharsetUtil.UTF_8);
    }

    private Optional<Object> evalByAdHoc(Element e, ByteBuf bbuf) {
        if (e.type.startsWith("unsigned")) {
            return Optional.of(readUnsignedNumValue(bbuf));
        } else if (e.type.startsWith("signed")) {
            return Optional.of(readNumValue(bbuf));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Object> readIpAddress(int size, ByteBuf bbuf) {
        byte[] ipBuffer;
        if (size == 4 && bbuf.readableBytes() == 4) {
            ipBuffer = buffer4.get();
        } else if (size == 16 && bbuf.readableBytes() == 16) {
            ipBuffer = buffer16.get();
        } else {
            return Optional.empty();
        }
        bbuf.readBytes(ipBuffer);
        try {
            return Optional.of(InetAddress.getByAddress(ipBuffer));
        } catch (UnknownHostException e) {
            // Should never be reached
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Object> decodeApplicationId(ByteBuf bbuf) {
        Map<String, Object> applicationId = new HashMap<>();
        byte classificationEngineID = bbuf.readByte();
        applicationId.put("ClassificationEngineID", classificationEngineID);
        // The encoding of the class PANA-L7-PEN is a little magique
        if (classificationEngineID == 20) {
            applicationId.put("PrivateEntrepriseNumber", bbuf.readInt());
        }
        applicationId.put("SelectorID", readUnsignedNumValue(bbuf));

        return applicationId;
    }

    private long readNumValue(ByteBuf bbuf) {
        switch (bbuf.readableBytes()) {
        case 0:
            return 0;
        case 1:
            return bbuf.readByte();
        case 2:
            return bbuf.readShort();
        case 4:
            return bbuf.readInt();
        case 8:
            return bbuf.readLong();
        default:
            if (bbuf.readableBytes() < 8) {
                byte[] bufNum = new byte[8];
                for (int i = (bbuf.readableBytes() - 1); i >= 0; i--) {
                    bufNum[i] = bbuf.getByte(i);
                }
                return Unpooled.wrappedBuffer(bufNum).readLong();
            } else {
                throw new IllegalStateException("Unreadable size :" + bbuf.readableBytes());
            }
        }
    }

    private long readUnsignedNumValue(ByteBuf bbuf) {
        switch (bbuf.readableBytes()) {
        case 0:
            return 0;
        case 1:
            return bbuf.readUnsignedByte();
        case 2:
            return bbuf.readUnsignedShort();
        case 4:
            return bbuf.readUnsignedInt();
        case 8:
            return bbuf.readLong();
        default:
            if (bbuf.readableBytes() < 8) {
                byte[] bufNum = new byte[8];
                for (int i = (bbuf.readableBytes() - 1); i >= 0; i--) {
                    bufNum[i] = bbuf.getByte(i);
                }
                return Unpooled.wrappedBuffer(bufNum).readLong();
            } else {
                throw new IllegalStateException("Unreadable size :" + bbuf.readableBytes());
            }
        }
    }

}
