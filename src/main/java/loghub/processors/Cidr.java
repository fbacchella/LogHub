package loghub.processors;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Data;
import lombok.Setter;

import static java.math.BigInteger.ONE;

@BuilderClass(Cidr.Builder.class)
public class Cidr extends FieldsProcessor {

    private static final BigInteger MAXIMUM_VALUE = ONE.shiftLeft(128).subtract(ONE);

    @Data
    private static class NetworkInfo {
        private final int[] mask;
        private final int[] network;
        private final String netname;
        public NetworkInfo(String netname, int bits) {
            try {
                InetAddress resolved = InetAddress.getByName(netname);
                this.netname = netname;
                if (resolved instanceof Inet4Address) {
                    int maskValue = (int)(((1L<<32) - 1) - (((1L<<32) >> bits) - 1));
                    int networkValue = ByteBuffer.wrap(resolved.getAddress()).getInt() & maskValue;
                    this.mask = new int[] {maskValue};
                    this.network =  new int[] {networkValue};
                } else if (resolved instanceof Inet6Address) {
                    BigInteger maskValue = MAXIMUM_VALUE.subtract(MAXIMUM_VALUE.shiftRight(bits));
                    BigInteger networkValue = new BigInteger(resolved.getAddress()).and(maskValue);
                    ByteBuffer maskBuffer = ByteBuffer.wrap(maskValue.toByteArray());
                    ByteBuffer networkBuffer = ByteBuffer.wrap(networkValue.toByteArray());
                    // byte array generated may contain one extra byte
                    if (maskBuffer.remaining() == 17) {
                        maskBuffer.position(1);
                    }
                    if (networkBuffer.remaining() == 17) {
                        networkBuffer.position(1);
                    }
                    mask = new int[] {maskBuffer.getInt(), maskBuffer.getInt(), maskBuffer.getInt(), maskBuffer.getInt()};
                    network = new int[] {networkBuffer.getInt(), networkBuffer.getInt(), networkBuffer.getInt(), networkBuffer.getInt()};
                } else {
                    throw new IllegalArgumentException("Unhandled address type");
                }
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Not an IP address: " + Helpers.resolveThrowableException(e), e);
            }
        }
    }

    public static class Builder extends FieldsProcessor.Builder<Cidr> {
        @Setter
        private String[] networks = new String[]{};
        public Cidr build() {
            return new Cidr(this);
        }
    }
    public static Cidr.Builder getBuilder() {
        return new Cidr.Builder();
    }

    private final Set<NetworkInfo> networks;
    public Cidr(Cidr.Builder builder) {
        super(builder);
        networks = Arrays.stream(builder.networks).map(this::mapNetworkString).collect(Collectors.toSet());
    }

    private static final Pattern ipPattern = Pattern.compile("(.*)/(\\d+)");
    private NetworkInfo mapNetworkString(String network) {
        Matcher m = ipPattern.matcher(network);
        if (m.matches()) {
            return new NetworkInfo(m.group(1), Integer.parseInt(m.group(2)));
        } else {
            throw new IllegalArgumentException("Not a valid net mask: " + network);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        InetAddress address;
        if (value instanceof InetAddress) {
            address = (InetAddress) value;
        } else {
            try {
                address = InetAddress.getByName(value.toString());
            } catch (UnknownHostException e) {
                throw event.buildException("Not an IP address: " + Helpers.resolveThrowableException(e), e);
            }
        }
        ByteBuffer buffer = ByteBuffer.wrap(address.getAddress());
        for (NetworkInfo n: networks) {
            buffer.rewind();
            boolean match = false;
            if (n.mask.length == 1 && address instanceof Inet4Address) {
                match = (n.mask[0] & buffer.getInt()) == n.network[0];
            } else if (n.mask.length == 4 && address instanceof Inet6Address) {
                for (int i = 0; i < 4; i++) {
                    match = (n.mask[i] & buffer.getInt()) == n.network[i];
                    if (!match) {
                        break;
                    }
                }
            }
            if (match) {
                return n.netname;
            }
        }
        return RUNSTATUS.FAILED;
     }

}
