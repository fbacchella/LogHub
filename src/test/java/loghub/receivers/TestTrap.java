package loghub.receivers;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.PDU;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import loghub.Event;
import loghub.LogUtils;
import loghub.NamedArrayBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestTrap {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.SnmpTrap", "loghub.Receiver", "loghub.SMIResolve");
    }

    @Test()
    public void testone() throws InterruptedException, IOException {
        NamedArrayBlockingQueue receiver = new NamedArrayBlockingQueue("out.listener1");
        SnmpTrap r = new SnmpTrap(receiver);
        r.setPort(1162);
        r.configure(new Properties(Collections.emptyMap()));
        System.out.println(r.smartPrint(new OID("1.0.8802.1.1.2.1.1.2.5")));
        r.interrupt();
    }

    @Test()
    public void testbig() throws InterruptedException, IOException {
        NamedArrayBlockingQueue receiver = new NamedArrayBlockingQueue("out.listener1");
        SnmpTrap r = new SnmpTrap(receiver);
        r.setPort(1162);
        r.configure(new Properties(Collections.emptyMap()));

        CommandResponderEvent trapEvent = new CommandResponderEvent(new MessageDispatcherImpl(), new DefaultUdpTransportMapping(), new GenericAddress(), 0,0, null, 0, null, null, 0, null );
        PDU pdu = new PDU();
        pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.4.1.1.8.207185300.2.15079"), new OctetString("vnet7")));
        pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.3.7.1.4.2"), new OctetString("eth0")));
        pdu.add(new VariableBinding(new OID("1.0.8802.1.1.2.1.4.1.1.9.207185300.2.15079"), new OctetString("ng50.prod.exalead.com")));
        pdu.add(new VariableBinding(new OID("1.3.6.1.6.3.1.1.4.1.0"), new OctetString("lldpRemTablesChange")));
        trapEvent.setPDU(pdu);

        r.processPdu(trapEvent);
        Event e = receiver.poll();
        Map<String,?> details = (Map<String, ?>) e.get("lldpRemSysName");
        logger.debug(Array.getLength(details.get("index")));
        r.interrupt();

    }
}
