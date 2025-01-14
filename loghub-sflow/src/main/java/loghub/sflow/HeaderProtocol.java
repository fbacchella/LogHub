package loghub.sflow;

public enum HeaderProtocol {
    UNKNOWN,
    ETHERNET_SO88023,
    ISO88024_TOKENBUS,
    ISO88025_TOKENRING,
    FDDI,
    FRAME_RELAY,
    X25,
    PPP,
    SMDS,
    AAL5,
    AAL5_IP, /* e.g. Cisco AAL5 mux */
    IPv4,
    IPv6,
    MPLS,
    POS  /* RFC 1662, 2615 */;

    public static HeaderProtocol parse(int protocol) {
        switch (protocol) {
        case 1: return ETHERNET_SO88023;
        case 2: return ISO88024_TOKENBUS;
        case 3: return ISO88025_TOKENRING;
        case 4: return FDDI;
        case 5: return FRAME_RELAY;
        case 6: return X25;
        case 7: return PPP;
        case 8: return SMDS;
        case 9: return AAL5;
        case 10: return AAL5_IP;
        case 11: return IPv4;
        case 12: return IPv6;
        case 13: return MPLS;
        case 14: return POS;
        default:
            return UNKNOWN;
        }
    }
}
