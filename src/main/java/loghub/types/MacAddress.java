package loghub.types;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.VarFormatter;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class MacAddress {

    private static final Pattern macPattern = Pattern.compile("^([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})[-:.]([0-9A-Fa-f]{2})");
    private static final ThreadLocal<Matcher> localMatcher = ThreadLocal.withInitial(() -> macPattern.matcher(""));
    private static final VarFormatter formatter = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}");

    private final Byte[] address = new Byte[6];

    public MacAddress(String addressStr) {
        Matcher m = localMatcher.get().reset(addressStr.trim());
        if (! m.matches()) {
            throw new IllegalArgumentException(addressStr + " is not a valid mac address");
        } else {
            for (int i=0; i < 6; i++) {
                //0x is to decode as a positive number
                address[i] = Short.decode("0x" + m.group(i + 1)).byteValue();
            }
        }
    }

    @Override
    @JsonValue
    public String toString() {
        return formatter.format(address);
    }

}
