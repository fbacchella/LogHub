package loghub.decoders;

import java.util.Map;

public enum BbdoCategory {
    NEB(1),
    BBDO(2),
    STORAGE(3),
    CORRELATION(4),
    DUMPER(5),
    BAM(6),
    EXTCMD(7),
    INTERNAL(65535);

    public final int value;

    BbdoCategory(int value) {
        this.value = value;
    }

    private static final Map<Integer, BbdoCategory> BY_VALUE;
    static {
        Map<Integer, BbdoCategory> map = new java.util.HashMap<>();
        for (BbdoCategory n : values()) {
            map.put(n.value, n);
        }
        BY_VALUE = Map.copyOf(map);
    }

    public static BbdoCategory fromValue(int value) {
        return BY_VALUE.get(value);
    }
}
