package loghub.receivers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import loghub.configuration.Beans;
import loghub.configuration.Properties;
import loghub.snmp.NaturalOrderComparator;
import loghub.snmp.OidTreeNode;

@Beans({"protocol", "port", "listen"})
public class SnmpTrap extends Receiver implements CommandResponder {

    private static final Logger logger = LogManager.getLogger();

    static private Snmp snmp;
    static private ThreadPool threadPool;
    static private OidTreeNode top = null;
    static private boolean reconfigured = false;

    private String protocol = "udp";
    private int port = 162;
    private String listen = "0.0.0.0";

    private static void loadOids(InputStream is) throws IOException {
        SortedMap<String, String> oids = new TreeMap<String, String>(new NaturalOrderComparator());
        java.util.Properties p = new java.util.Properties();
        p.load(is);
        for(Entry<Object, Object> e: p.entrySet()) {
            oids.put((String) e.getKey(), (String) e.getValue());
        }
        for(Entry<String, String> e: oids.entrySet()) {
            top.addOID(new OID(e.getKey()), e.getValue());
        }
    }

    static {
    }

    public SnmpTrap() throws IOException {
        threadPool = ThreadPool.create("Trap", 2);
        MultiThreadedMessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(threadPool,
                new MessageDispatcherImpl());
        dispatcher.addCommandResponder(this);
        Address listenAddress = GenericAddress.parse(protocol + ":" + listen + "/" + port);
        TransportMapping<?> transport = new DefaultUdpTransportMapping((UdpAddress) listenAddress);
        snmp = new Snmp(dispatcher, transport);
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv1());
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv2c());
        snmp.listen();
    }

    @Override
    public boolean configure(Properties properties) {
        if(top == null) {
            InputStream in = properties.classloader.getResourceAsStream("oid.properties");
            try {
                loadOids(in);
            } catch (IOException e) {
                logger.fatal("unable to load default oid: {}", e.getMessage());
                logger.catching(Level.DEBUG, e);
                return false;
            }
        }

        if(! reconfigured && properties.containsKey("oidfile")) {
            reconfigured = true;
            String oidfile = null;
            try {
                oidfile = (String) properties.get("oidfile");
                InputStream is = new FileInputStream(oidfile);
                loadOids(is);
            } catch (ClassCastException e) {
                logger.error("oidfile property is not a string");
                return false;
            } catch (FileNotFoundException e) {
                logger.error("oidfile {} cant't be found", oidfile);
                return false;
            } catch (IOException e) {
                logger.error("oidfile {} cant't be read:{}", oidfile, e.getMessage());
                return false;
            }
        }
        return super.configure(properties);
    }

    @Override
    public void run() {
        try {
            snmp.wait();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void processPdu(CommandResponderEvent trap) {
        @SuppressWarnings("unchecked")
        Enumeration<VariableBinding> vbenum = (Enumeration<VariableBinding>) trap.getPDU().getVariableBindings().elements();
        Event event = new Event();
        for(VariableBinding i: Collections.list(vbenum)) {
            OID vbOID = i.getOid();
            String key = smartPrint(vbOID);
            String value = i.toValueString();
            if(i.getVariable().getSyntax() == SMIConstants.SYNTAX_OBJECT_IDENTIFIER) {
                value = smartPrint( (OID) i.getVariable() );
            }
            event.put(key, value);
        }
        trap.setProcessed(true);
        send(event);
    }

    public String smartPrint(OID oid) {
        OidTreeNode found = top.search(oid);
        OID foundOID = found.getOID();
        String formatted;
        if (oid.startsWith(foundOID)) {
            int[] suffixes = Arrays.copyOfRange(oid.getValue(), foundOID.size(), oid.size());
            if(suffixes.length > 0) {
                formatted = found.getName() + "." + new OID(suffixes);
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

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getListen() {
        return listen;
    }

    public void setListen(String listen) {
        this.listen = listen;
    }

}
