package loghub.snmp;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.DtlsAddress;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.TlsAddress;
import org.snmp4j.smi.UdpAddress;

import loghub.IpConnectionContext;
import loghub.cloners.DeepCloner;
import lombok.Getter;

@Getter
public class SnmpConnectionContext extends IpConnectionContext {

    static {
        DeepCloner.registerImmutable(SnmpConnectionContext.class);
    }

    private final String version;
    private final String transport;

    public SnmpConnectionContext(InetSocketAddress localaddr, InetSocketAddress remoteaddr, int messageProcessingModel,
                                  TransportMapping<?> transportMapping) {
        super(localaddr, remoteaddr);
        this.version = resolveSnmpVersion(messageProcessingModel);
        this.transport = resolveTransport(transportMapping);
    }

    private static String resolveSnmpVersion(int messageProcessingModel) {
        return switch (messageProcessingModel) {
        case MessageProcessingModel.MPv1 -> "v1";
        case MessageProcessingModel.MPv2c -> "v2c";
        case MessageProcessingModel.MPv3 -> "v3";
        default -> Integer.toString(messageProcessingModel);
        };
    }

    private static String resolveTransport(TransportMapping<?> transportMapping) {
        Address listenAddress = transportMapping != null ? transportMapping.getListenAddress() : null;
        return switch (listenAddress) {
        case null -> null;
        case TlsAddress tlsAddress -> "tls";
        case DtlsAddress dtlsAddress -> "dtls";
        case TcpAddress tcpAddress -> "tcp";
        case UdpAddress udpAddress -> "udp";
        default -> listenAddress.getClass().getSimpleName();
        };
    }

    @Override
    public Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>(super.getProperties());
        props.put("version", version);
        props.put("transport", transport);
        return props;
    }

    @Override
    public <T> Optional<T> getProperty(String property) {
        return switch (property) {
        case "version" -> (Optional<T>) Optional.of(version);
        case "transport" -> (Optional<T>) Optional.ofNullable(transport);
        default -> super.getProperty(property);
        };
    }

}
