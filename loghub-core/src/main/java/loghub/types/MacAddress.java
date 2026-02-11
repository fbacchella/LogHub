package loghub.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.cloners.Immutable;
import loghub.VarFormatter;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@Immutable
public class MacAddress {

    private static final Pattern macPattern = Pattern.compile("([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2})(?:[-:.]([0-9A-Fa-f]{1,2})[-:.]([0-9A-Fa-f]{1,2}))?");
    private static final Pattern ibPattern = Pattern.compile("(IB )?GID: ([0-9a-fA-F:]+), QPN: (0x[0-9a-fA-F]+)");
    private static final ThreadLocal<Matcher> localMatcher = ThreadLocal.withInitial(() -> macPattern.matcher(""));
    private static final ThreadLocal<Matcher> localIbMatcher = ThreadLocal.withInitial(() -> ibPattern.matcher(""));
    private static final VarFormatter formatter48 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}");
    private static final VarFormatter formatter64 = new VarFormatter("${#1%02X}-${#2%02X}-${#3%02X}-${#4%02X}-${#5%02X}-${#6%02X}-${#7%02X}-${#8%02X}");

    private final byte[] address;

    public MacAddress(byte[] address) {
        if (address.length != 6 && address.length != 8 && address.length != 20) {
            throw new IllegalArgumentException("Invalid mac address length " + address.length);
        } else {
            this.address = Arrays.copyOf(address, address.length);
        }
    }

    public MacAddress(String addressStr) {
        String trimmed = addressStr.trim();
        Matcher m = localMatcher.get().reset(trimmed);
        Matcher mIb = localIbMatcher.get().reset(trimmed);
        if (m.matches()) {
            int length = 0;
            for (int i = 1; i <= m.groupCount(); i++) {
                if (m.group(i) == null) {
                    break;
                } else {
                    length = i;
                }
            }
            address = new byte[length];

            for (int i = 0; i < address.length; i++) {
                String j = m.group(i + 1);
                address[i] = Short.decode("0x" + j).byteValue();
            }
        } else if (mIb.matches()) {
            String gid = mIb.group(1);
            String qpnStr = mIb.group(2);
            byte[] gidBytes = parseGid(gid);
            long qpn = Long.decode(qpnStr);
            address = new byte[20];
            ByteBuffer bb = ByteBuffer.wrap(address);
            bb.put(gidBytes);
            bb.putInt((int) qpn);
        } else {
            throw new IllegalArgumentException(addressStr + " is not a valid mac address");
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
        } else if (address.length == 20) {
            ByteBuffer bb = ByteBuffer.wrap(address);
            byte[] gidBytes = new byte[16];
            bb.get(gidBytes);
            int qpn = bb.getInt();
            return String.format("<%s, 0x%06x>", formatGid(gidBytes), qpn);
        } else {
            throw new IllegalStateException("Invalid mac address length " + address.length);
        }
    }

    private byte[] parseGid(String gidStr) {
        String s = gidStr.trim();
        // Handle IPv6-like notation with optional '::' compression
        String[] parts;
        int[] words = new int[8];
        int idx = 0;
        if (s.contains("::")) {
            String[] halves = s.split("::", -1);
            if (halves.length != 2) {
                throw new IllegalArgumentException(gidStr + " is not a valid GID");
            }
            String left = halves[0];
            String right = halves[1];
            String[] leftParts = left.isEmpty() ? new String[0] : left.split(":");
            String[] rightParts = right.isEmpty() ? new String[0] : right.split(":");
            if (leftParts.length + rightParts.length > 8) {
                throw new IllegalArgumentException(gidStr + " is not a valid GID");
            }
            for (String lp : leftParts) {
                if (lp.isEmpty() || lp.length() > 4) throw new IllegalArgumentException(gidStr + " is not a valid GID");
                words[idx++] = Integer.parseInt(lp, 16);
            }
            int zerosToInsert = 8 - (leftParts.length + rightParts.length);
            for (int i = 0; i < zerosToInsert; i++) {
                words[idx++] = 0;
            }
            for (String rp : rightParts) {
                if (rp.isEmpty() || rp.length() > 4) throw new IllegalArgumentException(gidStr + " is not a valid GID");
                words[idx++] = Integer.parseInt(rp, 16);
            }
        } else {
            parts = s.split(":");
            if (parts.length != 8) {
                throw new IllegalArgumentException(gidStr + " is not a valid GID");
            }
            for (String p : parts) {
                if (p.isEmpty() || p.length() > 4) throw new IllegalArgumentException(gidStr + " is not a valid GID");
                words[idx++] = Integer.parseInt(p, 16);
            }
        }
        if (idx != 8) {
            throw new IllegalArgumentException(gidStr + " is not a valid GID");
        }
        byte[] out = new byte[16];
        for (int i = 0; i < 8; i++) {
            int w = words[i] & 0xFFFF;
            out[i * 2] = (byte) ((w >> 8) & 0xFF);
            out[i * 2 + 1] = (byte) (w & 0xFF);
        }
        return out;
    }

    private String formatGid(byte[] gidBytes) {
        if (gidBytes.length != 16) {
            throw new IllegalArgumentException("GID must be 16 bytes");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16; i += 2) {
            int w = ((gidBytes[i] & 0xFF) << 8) | (gidBytes[i + 1] & 0xFF);
            sb.append(Integer.toHexString(w));
            if (i < 14) sb.append(":");
        }
        return sb.toString();
    }
}
