package loghub.cbor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class AbstractIpTagHandler extends CborTagHandler<Object> {

    AbstractIpTagHandler(int tag, Class<?>... targetType) {
        super(tag, targetType);
    }

    @Override
    public Object parse(CBORParser p) throws IOException {
        int tag = p.getCurrentTag();
        if (p.currentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
            // ipvx-address
            byte[] buffer = p.getBinaryValue();
            return InetAddress.getByAddress(buffer);
        } else if (p.currentToken() == JsonToken.START_ARRAY) {
            JsonToken nextToken = p.nextToken();
            if (nextToken == JsonToken.VALUE_NUMBER_INT && p.getParsingContext().getExpectedLength() == 2) {
                // ipvx-prefix
                return ipPrefix(p, tag);
            } else if (p.currentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
                int length = p.getParsingContext().getExpectedLength();
                // ipvx-address-with-prefix
                return ipAddressWithPrefix(p, tag, length);
            } else {
                throw new IOException("Invalid input Inetaddress");
            }
        } else {
            throw new IOException("Invalid input Inetaddress");
        }
    }

    private String ipPrefix(CBORParser p, int tag) throws IOException {
        int prefixLength = p.getIntValue();
        JsonToken nextToken = p.nextToken();
        assert nextToken == JsonToken.VALUE_EMBEDDED_OBJECT;
        byte[] buffer = p.getBinaryValue();
        InetAddress addr = mask(buffer, tag, prefixLength);
        nextToken = p.nextToken();
        assert nextToken == JsonToken.END_ARRAY;
        return String.format("%s/%d", addr.getHostAddress(), prefixLength);
    }

    private String ipAddressWithPrefix(CBORParser p, int tag, int length) throws IOException {
        byte[] buffer = p.getBinaryValue();
        JsonToken nextToken = p.nextToken();
        int prefixLength;
        if (nextToken == JsonToken.VALUE_NULL) {
            prefixLength = tag == 54 ? 128 : 32;
        } else {
            prefixLength = p.getIntValue();
        }
        if (prefixLength < 0 || prefixLength > (tag == 54 ? 128 : 32)) {
            throw new IOException("Invalid input Inetaddress");
        }

        InetAddress addr = mask(buffer, tag, prefixLength);
        String interfaceIdentifier;
        if (length == 3) {
            nextToken = p.nextToken();
            if (nextToken.isNumeric()) {
                interfaceIdentifier = "%" + p.getIntValue();
            } else if (nextToken == JsonToken.VALUE_STRING){
                interfaceIdentifier = "%" + p.getValueAsString();
            } else if (nextToken == JsonToken.VALUE_EMBEDDED_OBJECT){
                interfaceIdentifier = "%" + new String(p.getBinaryValue(), StandardCharsets.UTF_8);
            } else {
                throw new IOException("Invalid input Inetaddress");
            }
        } else {
            interfaceIdentifier = "";
        }
        nextToken = p.nextToken();
        assert nextToken == JsonToken.END_ARRAY;
        return String.format("%s/%d%s", addr.getHostAddress(), prefixLength, interfaceIdentifier);
    }

    private InetAddress mask(byte[] buffer, int tag, int prefixLength) throws UnknownHostException {
        int prefixBytes = prefixLength / 8;
        int prefixBits = prefixLength% 8;
        if (prefixBytes < (tag == 54 ? 16 : 4)) {
            int mask = 0xFF << (8 - prefixBits) & 0xFF;
            if (buffer.length != (tag == 54 ? 16 : 4)) {
                buffer = Arrays.copyOf(buffer, tag == 54 ? 16 : 4);
            }
            if (prefixBytes + 1 < buffer.length) {
                Arrays.fill(buffer, prefixBytes + 1, buffer.length, (byte)0);
            }
            buffer[prefixBytes] = (byte) (buffer[prefixBytes] & mask);
        }
        return InetAddress.getByAddress(buffer);
    }

    @Override
    public CBORGenerator write(Object data, CBORGenerator p) throws IOException {
        if (data instanceof InetAddress) {
            byte[] buffer = ((InetAddress)data).getAddress();
            p.writeBytes(buffer, 0, buffer.length);
        }
        return p;
    }

}
