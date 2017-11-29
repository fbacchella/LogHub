package loghub.netflow;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

class IpfixInformationElements {

    static class Element{
        public final int elementId;
        public final String name;
        public final String type;
        public final String semantics;
        public final String status;
        public final String description;
        public final String units;
        public final String range;
        public final String references;
        public final String requester;
        public final String revision;
        public final String date;

        Element(Map<String, String> content) {
            elementId = Integer.parseInt(content.get("ElementID"));
            name = content.get("Name");
            type = content.get("Abstract Data Type");
            semantics = content.get("Data Type Semantics");
            status = content.get("Status");
            description = content.get("Description");
            units = content.get("Units");
            range = content.get("Range");
            references = content.get("References");
            requester = content.get("Requester");
            revision = content.get("Revision");
            date = content.get("Date");
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
            if(!Arrays.equals(address, other.address))
                return false;
            return true;
        }

        @Override
        public String toString() {
            StringJoiner j = new StringJoiner(":");
            // No Arrays.toStream for bytes array
            for(int i= 0 ; i < address.length ; i++) {
                j.add(String.format("%02x", address[i]));
            }
            return j.toString();
        }

    }

    // Downloaded from https://www.iana.org/assignments/ipfix/ipfix-information-elements.csv
    private static final String CSVSOURCE="ipfix-information-elements.csv";
    private static final Pattern RANGEPATTERN = Pattern.compile("\\d+-\\d+");

    public final Map<Integer, Element> elements;

    public IpfixInformationElements() throws IOException {
        Reader in = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(CSVSOURCE));
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        Map<Integer, Element> buildElements = new HashMap<>();
        Matcher mtch = RANGEPATTERN.matcher("");
        for (CSVRecord record : records) {
            if (mtch.reset(record.get(0)).matches()) {
                continue;
            }
            Element e = new Element(record.toMap());
            buildElements.put(e.elementId, e);
        }
        elements = Collections.unmodifiableMap(buildElements);
    }

    public String getName(int i) {
        return elements.containsKey(i) ? elements.get(i).name : Integer.toString(i);
    }

    private static final ThreadLocal<byte[]> buffer4 = ThreadLocal.withInitial(() -> new byte[4]);
    private static final ThreadLocal<byte[]> buffer16 = ThreadLocal.withInitial(() -> new byte[16]);

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
            } else if ("dateTimeMilliseconds".equals(e.type)) {
                long value = readNumValue(bbuf);
                return new Date(value);
            } else if (e.type.startsWith("unsigned")) {
                return readUnsignedNumValue(bbuf);
            } else if (e.type.startsWith("signed")) {
                return readNumValue(bbuf);
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
                byte[] buffer = new byte[6];
                bbuf.readBytes(buffer);
                return new MacAddress(buffer);
            } else if ("string".equals(e.type)) {
                byte[] buffer = new byte[bbuf.readableBytes()];
                bbuf.readBytes(buffer);
                return new String(buffer, CharsetUtil.UTF_8);
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
