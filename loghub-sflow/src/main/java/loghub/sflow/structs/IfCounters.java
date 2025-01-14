package loghub.sflow.structs;

import io.netty.buffer.ByteBuf;
import loghub.sflow.IANAifType;
import loghub.sflow.IfDirection;
import loghub.sflow.SflowParser;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class IfCounters extends Struct {

    public static final String NAME = "if_counters";

    private final long ifIndex;            // Index de l'interface (unsigned int)
    private final IANAifType ifType;             // Type de l'interface (unsigned int)
    private final long ifSpeed;           // Vitesse de l'interface (unsigned int)
    private final IfDirection ifDirection;        // Direction de l'interface (unsigned int)
    private final int ifStatus;
    private final long ifInOctets;        // Nombre d'octets reçus
    private final long ifInUcastPkts;     // Nombre de paquets unicast reçus
    private final long ifInMulticastPkts;     // Nombre de paquets multicast reçus
    private final long ifInBroadcastPkts;     // Nombre de paquets broadcast reçus
    private final long ifInDiscards;      // Nombre de paquets abandonnés reçus
    private final long ifInErrors;        // Nombre d'erreurs reçues
    private final long ifOutOctets;       // Nombre d'octets envoyés
    private final long ifOutUcastPkts;    // Nombre de paquets unicast envoyés
    private final long ifOutMulticastPkts;    // Nombre de paquets multicast envoyés
    private final long ifOutBroadcastPkts;    // Nombre de paquets broadcast envoyés
    private final long ifOutDiscards;     // Nombre de paquets abandonnés envoyés
    private final long ifOutErrors;       // Nombre d'erreurs envoyées
    private final int ifPromiscuousMode;       // Nombre d'erreurs envoyées

    public IfCounters(SflowParser df, ByteBuf buf) {
        super(df.getByName(NAME));
        ByteBuf buffer = extractData(buf);
        ifIndex = buffer.readUnsignedInt();
        ifType = IANAifType.resolve(buffer.readInt());
        ifSpeed = buffer.readLong();
        ifDirection = IfDirection.parse(buffer.readInt());
        ifStatus = buffer.readInt();
        ifInOctets = buffer.readLong();
        ifInUcastPkts = buffer.readUnsignedInt();
        ifInMulticastPkts = buffer.readUnsignedInt();
        ifInBroadcastPkts = buffer.readUnsignedInt();
        ifInDiscards = buffer.readUnsignedInt();
        ifInErrors = buffer.readUnsignedInt();
        ifOutOctets = buffer.readLong();
        ifOutUcastPkts = buffer.readUnsignedInt();
        ifOutMulticastPkts = buffer.readUnsignedInt();
        ifOutBroadcastPkts = buffer.readUnsignedInt();
        ifOutDiscards = buffer.readUnsignedInt();
        ifOutErrors = buffer.readUnsignedInt();
        ifPromiscuousMode = buffer.readInt();
    }

    @Override
    public String getName() {
        return NAME;
    }

}
