package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.CommunityTarget;
import org.snmp4j.MessageDispatcher;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Counter32;
import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.Null;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Opaque;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.TransportIpAddress;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import com.codahale.metrics.Meter;

import loghub.BeanChecks;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.ReceivedExceptionDescription;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;

class TestTrap {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.SnmpTrap", "loghub.receivers", "org.snmp4j");
        Configurator.setLevel("fr.jrds.SmiExtensions", Level.ERROR);
        SnmpTrap.resetMibDirs();
    }

    private void doTest(Supplier<PDU> getPdu, Consumer<CommandResponderEvent<UdpAddress>> dosecurity, Consumer<Event> checkEvent)
            throws IOException {
        doTest(null, getPdu, dosecurity, checkEvent);
    }

    private void doTest(VariablePath target, Supplier<PDU> getPdu, Consumer<CommandResponderEvent<UdpAddress>> dosecurity, Consumer<Event> checkEvent)
            throws IOException {
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        SnmpTrap.Builder builder = SnmpTrap.getBuilder();
        builder.setPort(0);
        builder.setEventsFactory(factory);
        if (target != null) {
            builder.setTarget(target);
        }

        try (SnmpTrap r = builder.build()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testbig", null));
            Map<String, Object> props = new HashMap<>();
            props.put("mibdirs", new Object[]{"/usr/share/snmp/mibs"});
            Assertions.assertTrue(r.configure(new Properties(props)));
            r.start();

            TransportMapping<UdpAddress> mapping = new DefaultUdpTransportMapping();
            UdpAddress addr = (UdpAddress) UdpAddress.parse("127.0.0.1/162");
            CommandResponderEvent<UdpAddress> trapEvent = new CommandResponderEvent<>(new MessageDispatcherImpl(), mapping, addr, 0, 0, null, 0, null,
                    null, 0, null);
            dosecurity.accept(trapEvent);
            PDU pdu = getPdu.get();
            trapEvent.setPDU(pdu);

            r.processPdu(trapEvent);
            Event e = receiver.poll();
            Assertions.assertNotNull(e);
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            checkEvent.accept(e);
            r.stopReceiving();
        }
    }

    private Snmp getSnmp() throws IOException {
        Snmp snmp = new Snmp();
        MessageDispatcher disp = snmp.getMessageDispatcher();
        disp.addMessageProcessingModel(new MPv1());
        disp.addMessageProcessingModel(new MPv2c());
        snmp.addTransportMapping(new DefaultUdpTransportMapping());
        snmp.addTransportMapping(new DefaultTcpTransportMapping());
        snmp.listen();
        return snmp;
    }

    private void sendPdu(SnmpTrap.PROTOCOL protocol, PDU pdu) throws IOException, InterruptedException {
        Properties props = new Properties(new HashMap<>(Map.of("mibdirs", new Object[]{"/usr/share/snmp/mibs"})));
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        SnmpTrap.Builder builder = SnmpTrap.getBuilder();
        builder.setPort(1161);
        builder.setProtocol(protocol);
        builder.setEventsFactory(factory);

        try (SnmpTrap r = builder.build();
             Snmp snmp = getSnmp()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testbig", null));
            Stats.registerReceiver(r);
            Assertions.assertTrue(r.configure(props));
            r.start();

            InetSocketAddress isa = InetSocketAddress.createUnresolved("127.0.0.1", 1161);
            TransportIpAddress address = (protocol == SnmpTrap.PROTOCOL.tcp)
                                                  ? new TcpAddress(InetAddress.getByName("127.0.0.1"), isa.getPort())
                                                  : new UdpAddress(InetAddress.getByName("127.0.0.1"), isa.getPort());
            Target<TransportIpAddress> snmpTarget = new CommunityTarget<>(address, new OctetString("loghub"));
            snmpTarget.setVersion(pdu.getType() == PDU.V1TRAP ? SnmpConstants.version1 : SnmpConstants.version2c);
            snmpTarget.setRetries(1);
            snmpTarget.setTimeout(100);
            snmp.send(pdu, snmpTarget);
            Event e = receiver.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertNotNull(e);
            long statBytes = Stats.getMetric(Receiver.class, "bytes", Meter.class).getCount();
            Assertions.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            if (pdu.getType() == PDU.NOTIFICATION) {
                Assertions.assertEquals("loghub", e.getConnectionContext().getPrincipal().getName());
                Assertions.assertTrue(statBytes > 100 && statBytes < 110);
            } else if (pdu.getType() == PDU.V1TRAP) {
                Assertions.assertEquals(41, statBytes);
            }
            r.stopReceiving();
        }
    }

    private PDU getv1trap() {
        PDUv1 pdu = new PDUv1();
        pdu.setType(PDU.V1TRAP);
        pdu.setEnterprise(new OID(".1.3.6.1.2.1.1.8"));
        pdu.setGenericTrap(PDUv1.ENTERPRISE_SPECIFIC);
        pdu.setSpecificTrap(1);
        pdu.setAgentAddress(new IpAddress(InetAddress.getLoopbackAddress()));
        return pdu;
    }

    private PDU getv2trap() {
        PDU pdu = new PDU();
        pdu.setType(PDU.NOTIFICATION);
        pdu.add(new VariableBinding(SnmpConstants.sysUpTime, new TimeTicks(ManagementFactory.getRuntimeMXBean().getUptime())));
        pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, new OID(".1.3.6.1.2.1.1.8")));
        pdu.add(new VariableBinding(SnmpConstants.snmpTrapAddress, new IpAddress(InetAddress.getLoopbackAddress())));
        pdu.add(new VariableBinding(new OID(".1.3.6.1.2.1.1.8"), new OctetString("Major")));
        return pdu;
    }

    @Test
    void testNetworkTrapv1Tcp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.tcp, getv1trap());
    }

    @Test
    void testNetworkTrapv1Udp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.udp, getv1trap());
    }

    @Test
    void testNetworkTrapv2Tcp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.tcp, getv2trap());
    }

    @Test
    void testNetworkTrapv2Udp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.udp, getv2trap());
    }

    @Test
    void testbig() throws IOException {
        doTest(() -> {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.4.1.1.8.207185300.2.15079"), new OctetString("vnet7")));
            pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.3.7.1.4.2"), new OctetString("eth0")));
            pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.4.1.1.9.207185300.2.15079"), new OctetString("localhost")));
            pdu.add(new VariableBinding(new OID("1.3.6.1.6.3.1.1.4.1"), new OctetString("lldpRemTablesChange")));
            pdu.add(new VariableBinding(new OID("1.3.6.1.4.1.9586.100.5.1.2.0"), new OctetString("LOGHUB")));
            pdu.add(new VariableBinding(new OID("enterprises.9586.100.5.2.3.1.4"), new Integer32(1)));
            return pdu;
        }, te -> {

        }, e -> {
            Assertions.assertEquals(3, e.size());
            @SuppressWarnings("unchecked")
            Map<String,?> iso = (Map<String, ?>) e.get("iso");
            Assertions.assertEquals("vnet7", iso.get("0.8802.1.1.2.1.4.1.1.8.207185300.2.15079"));
            Assertions.assertEquals("localhost", iso.get("0.8802.1.1.2.1.4.1.1.9.207185300.2.15079"));
            Assertions.assertEquals("eth0", iso.get("0.8802.1.1.2.1.3.7.1.4.2"));

            @SuppressWarnings("unchecked")
            Map<String,?> enterprises = (Map<String, ?>) e.get("enterprises");
            Assertions.assertEquals(1, enterprises.get("9586.100.5.2.3.1.4"));
            Assertions.assertEquals("LOGHUB", enterprises.get("9586.100.5.1.2.0"));

            Assertions.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
        });
    }

    @Test
    void testTrapv2() throws IOException {
        InetAddress localhost = InetAddress.getByName("127.0.0.1");
        doTest(() -> {
            PDU pdu = new PDU();
            pdu.setType(PDU.TRAP);
            pdu.add(new VariableBinding(new OID("1.3.6.1.6.3.1.1.4.1"), new OctetString("lldpRemTablesChange")));
            // Some undecoded values
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.1.0"), new OctetString("1")));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.2.0"), new OctetString("2")));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.3.0"), new OctetString("3\0")));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.4.0"), Null.instance));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.5.0"), new OID("enterprises")));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.6.0"), new IpAddress("127.0.0.1")));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.7.0"), new TimeTicks(111)));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.8.0"), new Opaque(new byte[]{(byte)0x9f, (byte)0x78, (byte)0x04, (byte)0x42, (byte)0xf6, (byte)0x00, (byte)0x00})));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.9.0"), new Opaque(new byte[]{(byte)0x9f, (byte)0x79, (byte)0x08, (byte)0x42, (byte)0xf6, (byte)0x00, (byte)0x00, (byte)0x42, (byte)0xf6, (byte)0x00, (byte)0x00})));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.10.0"), new Integer32(4)));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.11.0"), new Counter64(5)));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.12.0"), new UnsignedInteger32(6)));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.13.0"), new Gauge32(7)));
            pdu.add(new VariableBinding(new OID("1.3.6.2.1.4.20.1.14.0"), new Counter32(8)));
            pdu.add(new VariableBinding(new OID("1.3.6.1.2.1.7.5.1.1.0.0.0.0.123"), new IpAddress("127.0.0.1")));
            return pdu;
        }, te -> {
            te.setMessageProcessingModel(MessageProcessingModel.MPv2c);
            te.setSecurityName("loghub".getBytes());
        }, e -> {
            Assertions.assertNull(e.get("specific_trap"));
            Assertions.assertEquals("loghub", e.getConnectionContext().getPrincipal().getName());
            Assertions.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
            @SuppressWarnings("unchecked")
            Map<String, Object> dodValues = (Map<String, Object>) e.get("dod");
            Assertions.assertEquals("1", dodValues.get("2.1.4.20.1.1.0"));
            Assertions.assertEquals("2", dodValues.get("2.1.4.20.1.2.0"));
            Assertions.assertEquals("3", dodValues.get("2.1.4.20.1.3.0"));
            Assertions.assertNull(dodValues.get("2.1.4.20.1.4.0"));
            Assertions.assertEquals("enterprises", dodValues.get("2.1.4.20.1.5.0"));
            Assertions.assertEquals(localhost, dodValues.get("2.1.4.20.1.6.0"));
            Assertions.assertEquals(Duration.ofMillis(1110), dodValues.get("2.1.4.20.1.7.0"));
            Assertions.assertEquals(123.0f, dodValues.get("2.1.4.20.1.8.0"));
            Assertions.assertEquals(3.87028163190784E14, dodValues.get("2.1.4.20.1.9.0"));
            Assertions.assertEquals(4, dodValues.get("2.1.4.20.1.10.0"));
            Assertions.assertEquals(5L, dodValues.get("2.1.4.20.1.11.0"));
            Assertions.assertEquals(6L, dodValues.get("2.1.4.20.1.12.0"));
            Assertions.assertEquals(7L, dodValues.get("2.1.4.20.1.13.0"));
            Assertions.assertEquals(8L, dodValues.get("2.1.4.20.1.14.0"));
            @SuppressWarnings("unchecked")
            Map<String, Object> udpLocalAddress = (Map<String, Object>) e.get("udpLocalAddress");
            @SuppressWarnings("unchecked")
            Map<String, Object> index = (Map<String, Object>) udpLocalAddress.get("index");
            Assertions.assertEquals(123, index.get("udpLocalPort"));
            Assertions.assertEquals(localhost, udpLocalAddress.get("value"));
            Assertions.assertEquals("v2c", e.getConnectionContext().getProperty("snmpVersion").orElse(null));
        });
    }

    @Test
    void testtrapv1Generic() throws IOException {
        InetAddress addr = InetAddress.getByName("0.0.0.0");
        doTest(() -> {
            PDUv1 pdu = new PDUv1();
            pdu.setEnterprise(new OID("1.3.6.1.4.1.232"));
            pdu.setAgentAddress(new IpAddress());
            pdu.setGenericTrap(1);
            pdu.setTimestamp(10);
            pdu.add(new VariableBinding(new OID("1.3.6.1.6.3.1.1.4.1"), new OctetString("lldpRemTablesChange")));
            return pdu;
        }, te -> {

        }, e -> {
            Assertions.assertEquals(Duration.ofMillis(100), e.get("time_stamp"));
            Assertions.assertEquals("warmStart", e.get("generic_trap"));
            Assertions.assertEquals("enterprises.232", e.get("enterprise"));
            Assertions.assertNull(e.get("specific_trap"));
            Assertions.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
            Assertions.assertEquals(addr, e.get("agent_addr"));
            Assertions.assertEquals("v1", e.getConnectionContext().getProperty("snmpVersion").orElse(null));
        });
    }

    @Test
    void testtrapv1Specific() throws IOException {
        InetAddress addr = InetAddress.getByName("0.0.0.0");
        doTest(() -> {
            PDUv1 pdu = new PDUv1();
            pdu.setEnterprise(new OID("1.3.6.1.4.1.232"));
            pdu.setAgentAddress(new IpAddress());
            pdu.setGenericTrap(6);
            pdu.setSpecificTrap(6013);
            pdu.setTimestamp(10);
            return pdu;
        }, te -> {

        }, e -> {
            Assertions.assertEquals(Duration.ofMillis(100), e.get("time_stamp"));
            Assertions.assertEquals("enterpriseSpecific", e.get("generic_trap"));
            Assertions.assertEquals("enterprises.232", e.get("enterprise"));
            Assertions.assertEquals("6013", e.get("specific_trap"));
            Assertions.assertEquals(addr, e.get("agent_addr"));
        });
    }

    @Test
    void testtrapv1GenericWithTarget() throws IOException {
        InetAddress addr = InetAddress.getByName("0.0.0.0");
        VariablePath target = VariablePath.of("snmp");
        doTest(target, () -> {
            PDUv1 pdu = new PDUv1();
            pdu.setEnterprise(new OID("1.3.6.1.4.1.232"));
            pdu.setAgentAddress(new IpAddress());
            pdu.setGenericTrap(1);
            pdu.setTimestamp(10);
            pdu.add(new VariableBinding(new OID("1.3.6.1.6.3.1.1.4.1"), new OctetString("lldpRemTablesChange")));
            return pdu;
        }, te -> {

        }, e -> {
            Assertions.assertNull(e.get("time_stamp"));
            Assertions.assertNull(e.get("generic_trap"));
            @SuppressWarnings("unchecked")
            Map<String, Object> snmp = (Map<String, Object>) e.get("snmp");
            Assertions.assertNotNull(snmp);
            Assertions.assertEquals(Duration.ofMillis(100), snmp.get("time_stamp"));
            Assertions.assertEquals("warmStart", snmp.get("generic_trap"));
            Assertions.assertEquals("enterprises.232", snmp.get("enterprise"));
            Assertions.assertNull(snmp.get("specific_trap"));
            Assertions.assertEquals("lldpRemTablesChange", snmp.get("snmpTrapOID"));
            Assertions.assertEquals(addr, snmp.get("agent_addr"));
        });
    }

    @Test
    void testInvalidTrap() throws IOException {
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        SnmpTrap.Builder builder = SnmpTrap.getBuilder();
        builder.setPort(0);
        builder.setEventsFactory(factory);

        try (SnmpTrap r = builder.build()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testbig", null));
            Map<String, Object> props = new HashMap<>();
            props.put("mibdirs", new Object[]{"/usr/share/snmp/mibs"});
            Assertions.assertTrue(r.configure(new Properties(props)));
            r.start();

            TransportMapping<UdpAddress> mapping = new DefaultUdpTransportMapping();
            UdpAddress addr = (UdpAddress) UdpAddress.parse("127.0.0.1/162");
            CommandResponderEvent<UdpAddress> trapEvent = new CommandResponderEvent<>(new MessageDispatcherImpl(), mapping, addr, 0, 0, null, 0, null,
                    null, 0, null);
            // No PDU is set on the trap event, processing it must raise a RuntimeException.

            r.processPdu(trapEvent);

            Assertions.assertNull(receiver.poll());
            Assertions.assertEquals(1L, Stats.getMetric(Receiver.class, "error", Meter.class).getCount());
            ReceivedExceptionDescription red = Stats.getReceiverError().stream().findAny().orElse(null);
            Assertions.assertNotNull(red);
            Assertions.assertEquals(r.getReceiverName(), red.receiver());
            r.stopReceiving();
        }
    }

    @Test
    void test_loghub_receivers_SnmpTrap() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.SnmpTrap"
                , BeanChecks.BeanInfo.build("port", Integer.TYPE)
                , BeanChecks.BeanInfo.build("protocol", SnmpTrap.PROTOCOL.class)
                , BeanChecks.BeanInfo.build("rcvBuf", Integer.TYPE)
                , BeanChecks.BeanInfo.build("listen", String.class)
                , BeanChecks.BeanInfo.build("worker", Integer.TYPE)
                , BeanChecks.BeanInfo.build("withSSL", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                , BeanChecks.BeanInfo.build("SSLKeyAlias", String.class)
        );
    }

}
