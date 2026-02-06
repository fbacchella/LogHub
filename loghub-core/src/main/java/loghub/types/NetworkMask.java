package loghub.types;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.Helpers;

public record NetworkMask(InetAddress network, short prefixLength) {

    private static final Pattern IP_PATTERN = Pattern.compile("(.+)/(\\d+)");

    public NetworkMask(InetAddress network, short prefixLength) {
        byte[] bytes = network.getAddress();
        byte[] mask = new byte[bytes.length];

        if (bytes.length == 4 && prefixLength > 32 || bytes.length == 16 && prefixLength > 128) {
            throw new IllegalArgumentException("Prefix length bigger that network length");
        }

        int fullBytes = prefixLength / 8;
        int remainingBits = prefixLength % 8;
        // full 0xFF bytes
        for (int i = 0; i < fullBytes; i++) {
            mask[i] = (byte) 0xFF;
        }

        // partial byte if needed
        if (remainingBits > 0) {
            int shift = 8 - remainingBits;
            mask[fullBytes] = (byte) (0xFF << shift);
        }

        // apply mask
        byte[] networkBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            networkBytes[i] = (byte) (bytes[i] & mask[i]);
        }

        try {
            this.network = InetAddress.getByAddress(networkBytes);
        } catch (UnknownHostException e) {
            // Should not be reachable
            throw new IllegalStateException(e);
        }
        this.prefixLength = prefixLength;
    }

    public boolean inNetwork(InetAddress address) {
        // Must be same family (IPv4 / IPv6)
        if (address.getClass() != network.getClass()) {
            return false;
        }
        if (address instanceof Inet6Address addrinet6 && network instanceof Inet6Address netinet6) {
            NetworkInterface addrif = addrinet6.getScopedInterface();
            NetworkInterface netif = netinet6.getScopedInterface();
            if ( (addrif != null && netif != null) && !addrif.equals(netif)) {
                return false;
            }
        }
        byte[] addr = address.getAddress();
        byte[] net  = network.getAddress();

        int fullBytes = prefixLength / 8;
        int remainingBits = prefixLength % 8;

        // Compare full bytes
        for (int i = 0; i < fullBytes; i++) {
            if (addr[i] != net[i]) {
                return false;
            }
        }

        // Compare partial byte
        if (remainingBits > 0) {
            int mask = 0xFF << (8 - remainingBits);
            return (addr[fullBytes] & mask) == (net[fullBytes] & mask);
        }

        return true;
    }

    @Override
    @JsonValue
    public String toString() {
        return "%s/%s".formatted(network.getHostAddress(), prefixLength);
    }

    public static NetworkMask of(String netname, byte prefixLength) {
        try {
            InetAddress address = InetAddress.getByName(netname);

            return new NetworkMask(address, prefixLength);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Not an IP address: " + Helpers.resolveThrowableException(e), e);
        }
    }

    public static NetworkMask of(String netmask) {
        Matcher m = IP_PATTERN.matcher(netmask);
        if (m.matches()) {
            try {
                return new NetworkMask(InetAddress.getByName(m.group(1)), Short.parseShort(m.group(2)));
            } catch (UnknownHostException | NumberFormatException ex) {
                throw new IllegalArgumentException("Not a valid netmask: " + netmask, ex);
            }
        } else {
            throw new IllegalArgumentException("Not a valid netmask: " + netmask);
        }
    }

}
