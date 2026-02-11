package loghub.netcap;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import loghub.BuildableConnectionContext;
import loghub.cloners.Immutable;
import loghub.types.MacAddress;
import lombok.EqualsAndHashCode;

@Immutable
@EqualsAndHashCode(callSuper = false)
public class SllConnectionContext<T> extends BuildableConnectionContext<T> {

    private final SocketaddrSll<T> packetAddress;
    private final T receivedInterfaceAddress;
    private final String interfaceDisplayName;
    private final MacAddress interfaceHardwareAddress;

    @SuppressWarnings("unchecked")
    public SllConnectionContext(SocketaddrSll<T> packetAddress) {
        try {
            this.packetAddress = packetAddress;
            T receivedAddr = packetAddress.getAddr();
            NetworkInterface localInterface = NetworkInterface.getByIndex(packetAddress.getIfindex());
            if (localInterface != null) {
                this.interfaceDisplayName = localInterface.getDisplayName();
                byte[] hardwareAddress = localInterface.getHardwareAddress();
                this.interfaceHardwareAddress = (hardwareAddress != null && (hardwareAddress.length == 6 || hardwareAddress.length == 8 || hardwareAddress.length == 20)) ? new MacAddress(hardwareAddress) : null;
            } else {
                this.interfaceDisplayName = null;
                this.interfaceHardwareAddress = null;
            }
            if (receivedAddr instanceof InetAddress && localInterface != null) {
                // No MAC address on a tunnel interface
                receivedInterfaceAddress = (T) localInterface.getInterfaceAddresses().get(0).getAddress();
            } else if ((receivedAddr instanceof MacAddress || receivedAddr == null) && localInterface != null) {
                receivedInterfaceAddress = (T) interfaceHardwareAddress;
            } else {
                throw new IllegalArgumentException("Unhandled address type: " + packetAddress);
            }
        } catch (SocketException e) {
            throw new IllegalArgumentException("Broken packet address", e);
        }
    }

    @Override
    public T getLocalAddress() {
        return receivedInterfaceAddress;
    }

    public T getRemoteAddress() {
        return packetAddress.getAddr();
    }

    @Override
    public Map<String, Object> getProperties() {
        Map<String, Object> props = HashMap.newHashMap(5);
        if (interfaceDisplayName != null) {
            props.put("ifName", interfaceDisplayName);
        }
        if (interfaceHardwareAddress != null) {
            props.put("hardwareAddress", interfaceHardwareAddress);
        }
        props.put("pkttype", packetAddress.getPkttype());
        props.put("hatype", packetAddress.getHatype());
        props.put("protocol", packetAddress.getProtocol());
        return props;
    }

    @Override
    public String toString() {
        return "SllConnectionContext{"
                + "packetAddress=" + packetAddress.getAddr()
                + ", receivedInterfaceAddress=" + receivedInterfaceAddress
                + ", ifName=" + interfaceDisplayName
                + ", ifHardwareAddress=" + interfaceHardwareAddress
                + ", pkttype=" + packetAddress.getPkttype()
                + ", hatype=" + packetAddress.getHatype()
                + ", protocol=" + packetAddress.getProtocol()
                + '}';
    }

}
