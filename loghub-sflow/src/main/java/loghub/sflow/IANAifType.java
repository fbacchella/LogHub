package loghub.sflow;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

/**
 * Implementation as an enum of the module <a href = "https://www.iana.org/assignments/ianaiftype-mib/ianaiftype-mib">IANAifType-MIB</a>
 */
@Getter
public enum IANAifType {
    // Définition des types IANA
    OTHER(1, "Other"),
    REGULAR1822(2, "Regular1822"),
    HDH1822(3, "HDH1822"),
    DDN_X25(4, "DDN-X.25"),
    RFC877_X25(5, "RFC877-X.25"),
    ETHERNET_CSMACD(6, "Ethernet-CSMA/CD"),
    ISO88023_CSMACD(7, "ISO88023-CSMA/CD"),
    ISO88024_TOKENBUS(8, "ISO88024-TokenBus"),
    ISO88025_TOKENRING(9, "ISO88025-TokenRing"),
    ISO88026_MAN(10, "ISO88026-MAN"),
    STARLAN(11, "StarLAN"),
    PROTEON_10MBIT(12, "Proteon 10Mbit"),
    PROTEON_80MBIT(13, "Proteon 80Mbit"),
    HYPERCHANNEL(14, "HyperChannel"),
    FDDI(15, "FDDI"),
    LAP_B(16, "LAP-B"),
    SDLC(17, "SDLC"),
    DS1(18, "DS1"),
    E1(19, "E1"),
    BASIC_ISDN(20, "Basic ISDN"),
    PRIMARY_ISDN(21, "Primary ISDN"),
    PROP_POINT2POINT_SERIAL(22, "Proprietary Point-to-Point Serial"),
    PPP(23, "PPP"),
    SOFTWARE_LOOPBACK(24, "Software Loopback"),
    EON(25, "EON"),
    ETHERNET_3MBIT(26, "Ethernet 3Mbit"),
    NSIP(27, "NSIP"),
    SLIP(28, "SLIP"),
    ULTRA(29, "ULTRA"),
    DS3(30, "DS3"),
    SIP(31, "SIP"),
    FRAME_RELAY(32, "Frame Relay"),
    RS232(33, "RS-232"),
    ATM(37, "ATM"),
    SONET(39, "SONET"),
    VMWARENICTEAM(272, "VMware NIC Team"),
    P2POVERLAN(303, "Point to Point over LAN interface"),
    // Ajoutez ici d'autres types selon vos besoins...
    UNKNOWN(-1, "Unknown");

    // Champs
    private final int code;
    private final String description;

    // Map pour résoudre rapidement un code en type
    private static final Map<Integer, IANAifType> LOOKUP_MAP = new HashMap<>();

    // Remplissage de la map statique
    static {
        for (IANAifType type : IANAifType.values()) {
            LOOKUP_MAP.put(type.code, type);
        }
    }

    // Constructeur
    IANAifType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    // Méthode statique pour résoudre un code
    public static IANAifType resolve(int code) {
        return LOOKUP_MAP.getOrDefault(code, UNKNOWN);
    }

    @Override
    public String toString() {
        return description;
    }

}
