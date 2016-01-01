package loghub.receivers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration; 
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.SMIConstants;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import loghub.Event;
import loghub.Receiver;
import loghub.snmp.NaturalOrderComparator;
import loghub.snmp.OidTreeNode;

public class SnmpTrap extends Receiver implements CommandResponder {

    static public Snmp snmp;
    static public ThreadPool threadPool;
    static private OidTreeNode top;
    static {

        top = new OidTreeNode();

        SortedMap<String, String> oids = new TreeMap<String, String>(new NaturalOrderComparator());
        InputStream in = OidTreeNode.class.getClassLoader().getResourceAsStream("oid.properties");
        Properties p = new Properties();
        try {
            p.load(in);
            for(Entry<Object, Object> e: p.entrySet()) {
                oids.put((String) e.getKey(), (String) e.getValue());
            }
            for(Entry<String, String> e: oids.entrySet()) {
                top.addOID(new OID(e.getKey()), e.getValue());                
            }
        } catch (IOException e) {
        }
    }

    public SnmpTrap() {
        super();
        try {
            threadPool = ThreadPool.create("Trap", 10);
            MultiThreadedMessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(threadPool,
                    new MessageDispatcherImpl());

            Address listenAddress = GenericAddress.parse("udp:127.0.0.1/1162");
            TransportMapping<?> transport = new DefaultUdpTransportMapping((UdpAddress) listenAddress);
            snmp = new Snmp(dispatcher, transport);
            snmp.getMessageDispatcher().addMessageProcessingModel(new MPv1());
            snmp.getMessageDispatcher().addMessageProcessingModel(new MPv2c());
            snmp.listen();
        } catch (IOException e) {
        }
    }

    @Override
    public void run() {
        try {
            snmp.wait();
        } catch (InterruptedException e) {
        }
        System.out.println("stop SnmpTrap");
    }

    @Override
    public void processPdu(CommandResponderEvent trap) {
        @SuppressWarnings("unchecked")
        Enumeration<VariableBinding> vbenum = (Enumeration<VariableBinding>) trap.getPDU().getVariableBindings().elements();
        Event event = new Event();
        for(VariableBinding i: Collections.list(vbenum)) {
            OID vbOID = i.getOid();
            String key = SmartPrint(vbOID);
            String value = i.toValueString();
            if(i.getVariable().getSyntax() == SMIConstants.SYNTAX_OBJECT_IDENTIFIER) {
                value = SmartPrint( (OID) i.getVariable() );
            }
            event.put(key, value);
        }
        send(event);
    }

    public String SmartPrint(OID oid) {
        OidTreeNode found = top.search(oid);
        OID foundOID = found.getOID();
        String formatted;
        if (oid.startsWith(foundOID)) {
            int[] suffixes = Arrays.copyOfRange(oid.getValue(), foundOID.size(), oid.size());
            if(suffixes.length > 0) {
                formatted = found.getName() + "." +  new OID(suffixes);
            }
            else {
                formatted = found.getName();
            }
        }
        else {
            formatted = oid.toDottedString();
        }
        return formatted;
    }

    @Override
    public String getReceiverName() {
        return "SnmpTrap";
    }

}
