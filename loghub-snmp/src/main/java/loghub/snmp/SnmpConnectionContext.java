package loghub.snmp;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.snmp4j.mp.MessageProcessingModel;

import loghub.IpConnectionContext;
import loghub.cloners.DeepCloner;
import lombok.Getter;

@Getter
public class SnmpConnectionContext extends IpConnectionContext {

    static {
        DeepCloner.registerImmutable(SnmpConnectionContext.class);
    }

    private final String snmpVersion;

    public SnmpConnectionContext(InetSocketAddress localaddr, InetSocketAddress remoteaddr, int messageProcessingModel) {
        super(localaddr, remoteaddr);
        this.snmpVersion = resolveSnmpVersion(messageProcessingModel);
    }

    private static String resolveSnmpVersion(int messageProcessingModel) {
        return switch (messageProcessingModel) {
        case MessageProcessingModel.MPv1 -> "v1";
        case MessageProcessingModel.MPv2c -> "v2c";
        case MessageProcessingModel.MPv3 -> "v3";
        default -> Integer.toString(messageProcessingModel);
        };
    }

    @Override
    public Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>(super.getProperties());
        props.put("snmpVersion", snmpVersion);
        return props;
    }

    @Override
    public <T> Optional<T> getProperty(String property) {
        return switch (property) {
        case "snmpVersion" -> (Optional<T>) Optional.of(snmpVersion);
        default -> super.getProperty(property);
        };
    }

}
