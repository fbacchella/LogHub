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
        case "Reserved":
            return Optional.of(readByteArray(bbuf));
        default:
            return Optional.empty();
        }
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
