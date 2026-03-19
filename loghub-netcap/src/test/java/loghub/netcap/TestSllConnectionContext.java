package loghub.netcap;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import loghub.types.MacAddress;

public class TestSllConnectionContext {

    @Test
    public void testGetProperties() throws SocketException {
        NetworkInterface ni = Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                .filter(i -> {
                    try {
                        return i.getIndex() > 0;
                    } catch (Exception _) {
                        return false;
                    }
                })
                .findFirst()
                .orElse(null);

        if (ni == null) {
            Assertions.fail("No suitable network interface found for testing");
            return;
        }

        byte[] hw = ni.getHardwareAddress();
        MacAddress mac = (hw != null && (hw.length == 6 || hw.length == 8)) ? new MacAddress(hw) : null;
        SocketaddrSll<MacAddress> addr = new SocketaddrSll<>(SLL_PROTOCOL.ETH_P_ALL, ni.getIndex());

        SllConnectionContext<MacAddress> ctx = new SllConnectionContext<>(addr, ni.getDisplayName(), mac);

        Map<String, Object> props = ctx.getProperties();
        Assert.assertFalse(props.containsKey("ifIndex"));
        Assert.assertEquals(ni.getDisplayName(), props.get("ifName"));
        
        if (mac != null) {
            Assert.assertEquals(mac, props.get("hardwareAddress"));
        } else {
            Assert.assertNull(props.get("hardwareAddress"));
        }
    }
}
