package loghub.sflow;

public enum IfDirection {
    UNKNOWN,
    FULL_DUPLEX,
    HALF_DUPLEX,
    IN,
    OUT;
    public static IfDirection parse(int ifDirection) {
        switch (ifDirection) {
        case 1: return FULL_DUPLEX;
        case 2: return HALF_DUPLEX;
        case 3: return IN;
        case 4: return OUT;
        default:
            return UNKNOWN;
        }
    }
}
