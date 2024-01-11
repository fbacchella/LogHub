package loghub.types;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.VarFormatter;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class MacAddress {

    private static final Pattern macPattern = Pattern.compile("^([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})(?:[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2}))?$");
    private static final ThreadLocal<Matcher> localMatcher = ThreadLocal.withInitial(() -> macPattern.matcher(""));
    private static final VarFormatter formatter48 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}");
    private static final VarFormatter formatter64 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}-${#7%02X}-${#8%02X}");

    private final Byte[] address;

    public MacAddress(byte[] address) {
        if (address.length != 6 && address.length != 8) {
            throw new IllegalArgumentException("Invalid mac address length " + address.length);
        } else {
            this.address = IntStream.range(0, address.length).mapToObj(i -> address[i]).toArray(Byte[]::new);
        }
    }

    public MacAddress(String addressStr) {
        Matcher m = localMatcher.get().reset(addressStr.trim());
        if (! m.matches()) {
            throw new IllegalArgumentException(addressStr + " is not a valid mac address");
        } else {
            address = IntStream.range(1, m.groupCount() + 1).mapToObj(m::group).filter(Objects::nonNull).map(g -> Short.decode("0x" + g).byteValue()).toArray(Byte[]::new);
        }
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
