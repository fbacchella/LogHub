package loghub.snmp;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.snmp4j.mp.MessageProcessingModel;

import loghub.IpConnectionContext;
import loghub.cloners.DeepCloner;

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
        if (messageProcessingModel == MessageProcessingModel.MPv1) {
            return "v1";
        } else if (messageProcessingModel == MessageProcessingModel.MPv2c) {
            return "v2c";
        } else if (messageProcessingModel == MessageProcessingModel.MPv3) {
            return "v3";
        } else {
            return Integer.toString(messageProcessingModel);
        }
    }

    public String getSnmpVersion() {
        return snmpVersion;
    }

    @Override
    public Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>(super.getProperties());
        props.put("snmpVersion", snmpVersion);
        return props;
    }

    @Override
    public <T> Optional<T> getProperty(String property) {
        return "snmpVersion".equals(property) ? (Optional<T>) Optional.of(snmpVersion) : super.getProperty(property);
    }

}
