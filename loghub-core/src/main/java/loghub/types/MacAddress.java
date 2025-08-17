package loghub.types;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.VarFormatter;

public record MacAddress (byte[] address) {

    private static final Pattern macPattern = Pattern.compile("([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})(?:[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2}))?");
    private static final ThreadLocal<Matcher> localMatcher = ThreadLocal.withInitial(() -> macPattern.matcher(""));
    private static final VarFormatter formatter48 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}");
    private static final VarFormatter formatter64 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}-${#7%02X}-${#8%02X}");

    public MacAddress(byte[] address) {
        if (address.length != 6 && address.length != 8) {
            throw new IllegalArgumentException("Invalid mac address length " + address.length);
        } else {
            this.address = Arrays.copyOf(address, address.length);
        }
    }

    public MacAddress(String addressStr) {
        this(parseAddress(addressStr));
    }

    private static byte[] parseAddress(String addressStr) {
        Matcher m = localMatcher.get().reset(addressStr.trim());
        if (! m.matches()) {
            throw new IllegalArgumentException(addressStr + " is not a valid mac address");
        } else {
            int length = 0;
            for (int i = 1; i <= m.groupCount(); i++) {
                if (m.group(i) == null) {
                    break;
                } else {
                    length = i;
                }
            }
            byte[] addrBuffer = new byte[length];

            for (int i = 0; i < addrBuffer.length; i++) {
                String j = m.group(i + 1);
                addrBuffer[i] = Short.decode("0x" + j).byteValue();
            }
            return addrBuffer;
        }
    }

    public byte[] getBytes() {
        return Arrays.copyOf(address, address.length);
    }

    @Override
    @JsonValue
    public String toString() {
        if (address.length == 6) {
            return formatter48.format(address);
        } else if (address.length == 8) {
            return formatter64.format(address);
        } else {
            throw new IllegalStateException("Invalid mac address length " + address.length);
        }
    }

}
