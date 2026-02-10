package loghub.netcap;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
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

    @SuppressWarnings("unchecked")
    public SllConnectionContext(SocketaddrSll<T> packetAddress) {
        try {
            this.packetAddress = packetAddress;
            T receivedAddr = packetAddress.getAddr();
            NetworkInterface localInterface = NetworkInterface.getByIndex(packetAddress.getIfindex());
            if (receivedAddr instanceof InetAddress) {
                receivedInterfaceAddress = (T) InetAddress.getByAddress(localInterface.getHardwareAddress());
            } else if (receivedAddr instanceof MacAddress) {
                receivedInterfaceAddress = (T) new MacAddress(localInterface.getHardwareAddress());
            } else {
                throw new IllegalArgumentException("Unhandled address type: " + packetAddress);
            }
        } catch (SocketException | UnknownHostException e) {
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
        return Map.of(
                "ifIndex", packetAddress.getIfindex(),
                "pkttype", packetAddress.getPkttype(),
                "hatype", packetAddress.getHatype(),
                "protocol", packetAddress.getProtocol());
    }

    @Override
    public String toString() {
        return "SllConnectionContext{" + "packetAddress=" + packetAddress.getAddr() + ", receivedInterfaceAddress=" + receivedInterfaceAddress + ", ifIndex=" + packetAddress.getIfindex() + ", pkttype=" + packetAddress.getPkttype() + ", hatype=" + packetAddress.getHatype() + ", protocol=" + packetAddress.getProtocol() + '}';
    }

}
