package loghub.netflow;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

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
import lombok.Data;

class IpfixInformationElements {

    @Data
    @JsonPropertyOrder({"elementId","name","type","semantics","status","description",
                        "units","range","additional","references","revision","date"})
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

    private static class MacAddress {
        private final byte[] address;

        public MacAddress(byte[] address) {
            this.address = address;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(address);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            MacAddress other = (MacAddress) obj;
            return Arrays.equals(address, other.address);
        }

        @Override
        public String toString() {
            StringJoiner j = new StringJoiner(":");
            // No Arrays.toStream for bytes array
            for (byte b : address) {
                j.add(String.format("%02x", b));
            }
            return j.toString();
        }

    }

    private static final ThreadLocal<byte[]> buffer4 = ThreadLocal.withInitial(() -> new byte[4]);
    private static final ThreadLocal<byte[]> buffer16 = ThreadLocal.withInitial(() -> new byte[16]);

    // Downloaded from https://www.iana.org/assignments/ipfix/ipfix-information-elements.csv
    private static final String CSVSOURCE="ipfix-information-elements.csv";

    public final Map<Integer, Element> elements;

    public IpfixInformationElements() throws IOException {
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
                    // skip failing lines
                }
            }
            elements = Collections.unmodifiableMap(buildElements);
        }
    }

    public String getName(int i) {
        return elements.containsKey(i) ? elements.get(i).name : Integer.toString(i);
    }

    public Object getValue(int i, ByteBuf bbuf) {
        try {
            Element e = elements.get(i);
            if (e == null) {
                byte[] buffer = new byte[bbuf.readableBytes()];
                bbuf.readBytes(buffer);
                return buffer;
            }
            if ("ipv4Address".equals(e.type) && bbuf.isReadable(4)) {
                bbuf.readBytes(buffer4.get());
                return InetAddress.getByAddress(buffer4.get());
            } else if ("ipv6Address".equals(e.type) && bbuf.isReadable(16)) {
                bbuf.readBytes(buffer16.get());
                return InetAddress.getByAddress(buffer16.get());
            } else if ("dateTimeSeconds".equals(e.type)) {
                long value = readNumValue(bbuf);
                return Instant.ofEpochSecond(value);
            } else if ("dateTimeMilliseconds".equals(e.type)) {
                long value = readNumValue(bbuf);
                return Instant.ofEpochMilli(value);
            } else if ("dateTimeMicroseconds".equals(e.type)) {
                long value = readNumValue(bbuf);
                return Instant.ofEpochSecond(0, value * 1000);
            } else if ("dateTimeNanoseconds".equals(e.type)) {
                long value = readNumValue(bbuf);
                return Instant.ofEpochSecond(0, value);
            } else if ("float64".equals(e.type) && bbuf.isReadable(8)) {
                return bbuf.readDouble();
            } else if (e.type.startsWith("unsigned")) {
                return readUnsignedNumValue(bbuf);
            } else if (e.type.startsWith("signed")) {
                return readNumValue(bbuf);
            } else if ("boolean".equals(e.type) && bbuf.isReadable(8)) {
                byte value = bbuf.readByte();
                return value == 1;
            } else if ("applicationId".equals(e.name)) {
                byte[] buffer = new byte[bbuf.readableBytes()];
                bbuf.readBytes(buffer);
                Map<String, Number> applicationId = new HashMap<>();
                applicationId.put("ClassificationEngineID", buffer[0]);
                buffer[0] = 0;
                ByteBuf selectorBuffer = Unpooled.wrappedBuffer(buffer);
                applicationId.put("SelectorID", readUnsignedNumValue(selectorBuffer));
                return applicationId;
            } else if ("octetArray".equals(e.type) || "Reserved".equals(e.name)) {
                byte[] buffer = new byte[bbuf.readableBytes()];
                bbuf.readBytes(buffer);
                return buffer;
            } else if ("macAddress".equals(e.type) && bbuf.isReadable(6)) {
                // newly allocated byte[] as it will be stored directly in MacAddress
                byte[] buffer = new byte[6];
                bbuf.readBytes(buffer);
                return new MacAddress(buffer);
            } else if ("string".equals(e.type)) {
                return bbuf.toString(CharsetUtil.UTF_8);
            } else {
                throw new RuntimeException("unmannage type: " + e.name);
            }
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
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
            throw new RuntimeException("Unreadable size :" + bbuf.readableBytes());
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
            throw new RuntimeException("Unreadable size :" + bbuf.readableBytes());
        }
    }

}
