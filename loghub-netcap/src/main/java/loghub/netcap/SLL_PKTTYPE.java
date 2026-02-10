package loghub.netcap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

public enum SLL_PKTTYPE {

    PACKET_HOST(0),
    PACKET_BROADCAST(1),
    PACKET_MULTICAST(2),
    PACKET_OTHERHOST(3),
    PACKET_OUTGOING(4),
    PACKET_LOOPBACK(5),
    PACKET_USER(6),
    PACKET_KERNEL(7);

    private static final Map<Byte, SLL_PKTTYPE> BY_VALUE;
    static {
        Map<Byte, SLL_PKTTYPE> map = new HashMap<>();
        for (SLL_PKTTYPE p : values()) {
            map.put(p.value, p);
        }
        BY_VALUE = Collections.unmodifiableMap(map);
    }

    @Getter
    private final byte value;

    SLL_PKTTYPE(int value) {
        this.value = (byte) value;
    }

    public static SLL_PKTTYPE fromValue(byte value) {
        return BY_VALUE.get(value);
    }

}
