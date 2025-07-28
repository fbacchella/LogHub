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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
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
import org.snmp4j.smi.Counter64;
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
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import com.codahale.metrics.Meter;

import loghub.BeanChecks;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.security.ssl.ClientAuthentication;

public class TestTrap {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.SnmpTrap", "loghub.receivers", "org.snmp4j");
        Configurator.setLevel("fr.jrds.SmiExtensions", Level.ERROR);
        SnmpTrap.resetMibDirs();
    }

    private void doTest(Supplier<PDU> getPdu, Consumer<CommandResponderEvent<UdpAddress>> dosecurity, Consumer<Event> checkEvent)
            throws IOException {
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        SnmpTrap.Builder builder = SnmpTrap.getBuilder();
        builder.setPort(0);
        builder.setEventsFactory(factory);

        try (SnmpTrap r = builder.build()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testbig", null));
            Map<String, Object> props = new HashMap<>();
            props.put("mibdirs", new Object[]{"/usr/share/snmp/mibs"});
            Assert.assertTrue(r.configure(new Properties(props)));
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
            Assert.assertNotNull(e);
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
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
        Stats.reset();
        PriorityBlockingQueue receiver = new PriorityBlockingQueue();
        SnmpTrap.Builder builder = SnmpTrap.getBuilder();
        builder.setPort(1161);
        builder.setProtocol(protocol);
        builder.setEventsFactory(factory);

        try (SnmpTrap r = builder.build();
             Snmp snmp = getSnmp()) {
            r.setOutQueue(receiver);
            r.setPipeline(new Pipeline(Collections.emptyList(), "testbig", null));
            Map<String, Object> props = new HashMap<>();
            props.put("mibdirs", new Object[]{"/usr/share/snmp/mibs"});
            Assert.assertTrue(r.configure(new Properties(props)));
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
            Assert.assertNotNull(e);
            long statBytes = Stats.getMetric(Meter.class, Receiver.class, "bytes").getCount();
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            if (pdu.getType() == PDU.NOTIFICATION) {
                Assert.assertEquals("loghub", e.getConnectionContext().getPrincipal().getName());
                Assert.assertTrue(statBytes > 100 && statBytes < 110);
            } else if (pdu.getType() == PDU.V1TRAP) {
                Assert.assertEquals(41, statBytes);
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
    public void testNetworkTrapv1Tcp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.tcp, getv1trap());
    }

    @Test
    public void testNetworkTrapv1Udp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.udp, getv1trap());
    }

    @Test
    public void testNetworkTrapv2Tcp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.tcp, getv2trap());
    }

    @Test
    public void testNetworkTrapv2Udp() throws IOException, InterruptedException {
        sendPdu(SnmpTrap.PROTOCOL.udp, getv2trap());
    }

    @Test
    public void testbig() throws IOException {
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
            Assert.assertEquals(3, e.size());
            @SuppressWarnings("unchecked")
            Map<String,?> iso = (Map<String, ?>) e.get("iso");
            Assert.assertEquals("vnet7", iso.get("0.8802.1.1.2.1.4.1.1.8.207185300.2.15079"));
            Assert.assertEquals("localhost", iso.get("0.8802.1.1.2.1.4.1.1.9.207185300.2.15079"));
            Assert.assertEquals("eth0", iso.get("0.8802.1.1.2.1.3.7.1.4.2"));

            @SuppressWarnings("unchecked")
            Map<String,?> enterprises = (Map<String, ?>) e.get("enterprises");
            Assert.assertEquals(1, enterprises.get("9586.100.5.2.3.1.4"));
            Assert.assertEquals("LOGHUB", enterprises.get("9586.100.5.1.2.0"));

            Assert.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
        });
    }

    @Test
    public void testTrapv2() throws IOException {
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
            pdu.add(new VariableBinding(new OID("1.3.6.1.2.1.7.5.1.1.0.0.0.0.123"), new IpAddress("127.0.0.1")));
            return pdu;
        }, te -> {
            te.setMessageProcessingModel(MessageProcessingModel.MPv2c);
            te.setSecurityName("loghub".getBytes());
        }, e -> {
            Assert.assertNull(e.get("specific_trap"));
            Assert.assertEquals("loghub", e.getConnectionContext().getPrincipal().getName());
            Assert.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
            @SuppressWarnings("unchecked")
            Map<String, Object> dodValues = (Map<String, Object>) e.get("dod");
            Assert.assertEquals("1", dodValues.get("2.1.4.20.1.1.0"));
            Assert.assertEquals("2", dodValues.get("2.1.4.20.1.2.0"));
            Assert.assertEquals("3", dodValues.get("2.1.4.20.1.3.0"));
            Assert.assertNull(dodValues.get("2.1.4.20.1.4.0"));
            Assert.assertEquals("enterprises", dodValues.get("2.1.4.20.1.5.0"));
            Assert.assertEquals(localhost, dodValues.get("2.1.4.20.1.6.0"));
            Assert.assertEquals(Duration.ofMillis(1110), dodValues.get("2.1.4.20.1.7.0"));
            Assert.assertEquals(123.0f, dodValues.get("2.1.4.20.1.8.0"));
            Assert.assertEquals(3.87028163190784E14, dodValues.get("2.1.4.20.1.9.0"));
            Assert.assertEquals(4, dodValues.get("2.1.4.20.1.10.0"));
            Assert.assertEquals(5L, dodValues.get("2.1.4.20.1.11.0"));
            @SuppressWarnings("unchecked")
            Map<String, Object> udpLocalAddress = (Map<String, Object>) e.get("udpLocalAddress");
            @SuppressWarnings("unchecked")
            Map<String, Object> index = (Map<String, Object>) udpLocalAddress.get("index");
            Assert.assertEquals(123, index.get("udpLocalPort"));
            Assert.assertEquals(localhost, udpLocalAddress.get("value"));
        });
    }

    @Test
    public void testtrapv1Generic() throws IOException {
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
            Assert.assertEquals(0.1, (Double)e.get("time_stamp"), 1e-10);
            Assert.assertEquals("warmStart", e.get("generic_trap"));
            Assert.assertEquals("enterprises.232", e.get("enterprise"));
            Assert.assertNull(e.get("specific_trap"));
            Assert.assertEquals("lldpRemTablesChange", e.get("snmpTrapOID"));
            Assert.assertEquals(addr, e.get("agent_addr"));
        });
    }

    @Test
    public void testtrapv1Specific() throws IOException {
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
            Assert.assertEquals(0.1, (Double)e.get("time_stamp"), 1e-10);
            Assert.assertNull(e.get("generic_trap"));
            Assert.assertEquals("enterprises.232", e.get("enterprise"));
            Assert.assertEquals("enterprises.232 = 6013", e.get("specific_trap"));
            Assert.assertEquals(addr, e.get("agent_addr"));
        });
    }

    @Test
    public void test_loghub_receivers_SnmpTrap() throws IntrospectionException, ReflectiveOperationException {
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
