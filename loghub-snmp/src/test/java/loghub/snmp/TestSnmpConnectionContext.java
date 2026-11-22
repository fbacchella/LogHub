package loghub.snmp;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.snmp4j.mp.MessageProcessingModel;

class TestSnmpConnectionContext {

    private final InetSocketAddress localaddr = InetSocketAddress.createUnresolved("127.0.0.1", 161);
    private final InetSocketAddress remoteaddr = InetSocketAddress.createUnresolved("127.0.0.2", 1162);

    @Test
    void testSnmpVersionV1() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1);
        Assertions.assertEquals("v1", ctx.getSnmpVersion());
    }

    @Test
    void testSnmpVersionV2c() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv2c);
        Assertions.assertEquals("v2c", ctx.getSnmpVersion());
    }

    @Test
    void testSnmpVersionV3() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv3);
        Assertions.assertEquals("v3", ctx.getSnmpVersion());
    }

    @Test
    void testSnmpVersionUnknown() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, 42);
        Assertions.assertEquals("42", ctx.getSnmpVersion());
    }

    @Test
    void testGetProperties() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv2c);
        Map<String, Object> properties = ctx.getProperties();
        Assertions.assertEquals("v2c", properties.get("snmpVersion"));
    }

    @Test
    void testGetPropertySnmpVersion() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv3);
        Optional<String> snmpVersion = ctx.getProperty("snmpVersion");
        Assertions.assertTrue(snmpVersion.isPresent());
        Assertions.assertEquals("v3", snmpVersion.get());
    }

    @Test
    void testGetPropertyUnknown() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1);
        Optional<Object> unknown = ctx.getProperty("unknown");
        Assertions.assertFalse(unknown.isPresent());
    }

    @Test
    void testInheritedAddresses() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1);
        Assertions.assertEquals(localaddr, ctx.getLocalAddress());
        Assertions.assertEquals(remoteaddr, ctx.getRemoteAddress());
    }

}
