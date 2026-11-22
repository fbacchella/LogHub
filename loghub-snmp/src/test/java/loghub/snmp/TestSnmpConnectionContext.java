package loghub.snmp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

class TestSnmpConnectionContext {

    private final InetSocketAddress localaddr = InetSocketAddress.createUnresolved("127.0.0.1", 161);
    private final InetSocketAddress remoteaddr = InetSocketAddress.createUnresolved("127.0.0.2", 1162);

    private final TransportMapping<?> udpMapping = newUdpTransportMapping();
    private final TransportMapping<?> tcpMapping = newTcpTransportMapping();

    private static TransportMapping<?> newUdpTransportMapping() {
        try {
            return new DefaultUdpTransportMapping();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static TransportMapping<?> newTcpTransportMapping() {
        try {
            return new DefaultTcpTransportMapping();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Test
    void testVersionV1() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, udpMapping);
        Assertions.assertEquals("v1", ctx.getVersion());
    }

    @Test
    void testVersionV2c() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv2c, udpMapping);
        Assertions.assertEquals("v2c", ctx.getVersion());
    }

    @Test
    void testVersionV3() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv3, udpMapping);
        Assertions.assertEquals("v3", ctx.getVersion());
    }

    @Test
    void testVersionUnknown() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, 42, udpMapping);
        Assertions.assertEquals("42", ctx.getVersion());
    }

    @Test
    void testTransportUdp() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, udpMapping);
        Assertions.assertEquals("udp", ctx.getTransport());
    }

    @Test
    void testTransportTcp() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, tcpMapping);
        Assertions.assertEquals("tcp", ctx.getTransport());
    }

    @Test
    void testTransportNullMapping() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, null);
        Assertions.assertNull(ctx.getTransport());
    }

    @Test
    void testGetProperties() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv2c, udpMapping);
        Map<String, Object> properties = ctx.getProperties();
        Assertions.assertEquals("v2c", properties.get("version"));
        Assertions.assertEquals("udp", properties.get("transport"));
    }

    @Test
    void testGetPropertyVersion() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv3, udpMapping);
        Optional<String> version = ctx.getProperty("version");
        Assertions.assertTrue(version.isPresent());
        Assertions.assertEquals("v3", version.get());
    }

    @Test
    void testGetPropertyTransport() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv3, tcpMapping);
        Optional<String> transport = ctx.getProperty("transport");
        Assertions.assertTrue(transport.isPresent());
        Assertions.assertEquals("tcp", transport.get());
    }

    @Test
    void testGetPropertyUnknown() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, udpMapping);
        Optional<Object> unknown = ctx.getProperty("unknown");
        Assertions.assertFalse(unknown.isPresent());
    }

    @Test
    void testInheritedAddresses() {
        SnmpConnectionContext ctx = new SnmpConnectionContext(localaddr, remoteaddr, MessageProcessingModel.MPv1, udpMapping);
        Assertions.assertEquals(localaddr, ctx.getLocalAddress());
        Assertions.assertEquals(remoteaddr, ctx.getRemoteAddress());
    }

}
