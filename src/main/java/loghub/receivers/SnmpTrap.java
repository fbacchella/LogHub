package loghub.receivers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
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
import org.snmp4j.asn1.BER;
import org.snmp4j.asn1.BERInputStream;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Opaque;
import org.snmp4j.smi.SMIConstants;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import loghub.Event;
import loghub.NamedArrayBlockingQueue;
import loghub.Receiver;
import loghub.configuration.Beans;
import loghub.configuration.Properties;
import loghub.snmp.NaturalOrderComparator;
import loghub.snmp.OidTreeNode;

@Beans({"protocol", "port", "listen"})
public class SnmpTrap extends Receiver implements CommandResponder {

    private static final Logger logger = LogManager.getLogger();

    static final private byte TAG1 = (byte) 0x9f;
    static final private byte TAG_FLOAT = (byte) 0x78;
    static final private byte TAG_DOUBLE = (byte) 0x79;

    static private Snmp snmp;
    static private OidTreeNode top = null;
    static private boolean reconfigured = false;

    private ThreadPool threadPool;
    private String protocol = "udp";
    private int port = 162;
    private String listen = "0.0.0.0";

    public SnmpTrap(NamedArrayBlockingQueue outQueue) {
        super(outQueue);
    }

    @Override
    public boolean configure(Properties properties) {
        threadPool = ThreadPool.create("Trap", 2);
        MultiThreadedMessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(threadPool,
                new MessageDispatcherImpl());
        dispatcher.addCommandResponder(this);
        Address listenAddress = GenericAddress.parse(protocol + ":" + listen + "/" + port);
        TransportMapping<?> transport;
        try {
            transport = new DefaultUdpTransportMapping((UdpAddress) listenAddress);
        } catch (IOException e) {
            logger.error("can't bind to {}: {}", listenAddress, e.getMessage());
            return false;
        }
        snmp = new Snmp(dispatcher, transport);
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv1());
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv2c());
        try {
            snmp.listen();
        } catch (IOException e) {
            logger.error("can't listen: {}", e.getMessage());
        }
        if(top == null) {
            top = new OidTreeNode();
            try {
                Collections.list(properties.classloader.getResources("oid.properties"))
                .stream()
                .forEach(i -> {
                    try {
                        InputStream in = i.openStream();
                        loadOids(in);
                    } catch (IOException e) {
                        logger.error("unable to load oid from {}: {}", i, e.getMessage());
                        logger.catching(Level.DEBUG, e);
                    }
                });
            } catch (IOException e) {
                logger.error("unable to found oid.properties: {}", e.getMessage());
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
            synchronized(snmp) {
                snmp.wait();
            }
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void processPdu(CommandResponderEvent trap) {
        @SuppressWarnings("unchecked")
        Enumeration<VariableBinding> vbenum = (Enumeration<VariableBinding>) trap.getPDU().getVariableBindings().elements();
        Event event = new Event();
        Address addr = trap.getPeerAddress();
        if(addr instanceof IpAddress) {
            event.put("host", ((IpAddress)addr).getInetAddress());
        }
        for(VariableBinding i: Collections.list(vbenum)) {
            OID vbOID = i.getOid();
            Object value = convertVar(i.getVariable());
            smartPut(event, vbOID, value);
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

    private void smartPut(Event e, OID oid, Object value) {
        OidTreeNode found = top.search(oid);
        OID foundOID = found.getOID();
        if (oid.startsWith(foundOID)) {
            int[] suffixes = Arrays.copyOfRange(oid.getValue(), foundOID.size(), oid.size());
            if(suffixes.length > 0) {
                if(suffixes.length != 1 || suffixes[0] != 0) {
                    Map<String, Object> valueMap = new HashMap<>(2);
                    valueMap.put("indexoid", new OID(suffixes).toDottedString());
                    valueMap.put("value", value);
                    e.put(found.getName(), valueMap);
                } else {
                    e.put(found.getName(), value);
                }
            }
            else {
                e.put(found.getName(), value);
            }
        }
        else {
            e.put(oid.toDottedString(), value);
        }
    }

    private Object convertVar(Variable var) {
        if(var == null) {
            return null;
        }
        Object retvalue = null;
        switch(var.getSyntax()) {
        case 1 ://BOOLEAN
            return var.toInt();
        case SMIConstants.SYNTAX_INTEGER:
            return var.toInt();
        case 3: //BIT STRING
            break;
        case SMIConstants.SYNTAX_OCTET_STRING:
            //It might be a C string, try to remove the last 0;
            //But only if the new string is printable
            OctetString octetVar = (OctetString)var;
            int length = octetVar.length();
            if(length > 1 && octetVar.get(length - 1) == 0) {
                OctetString newVar = octetVar.substring(0, length - 1);
                if(newVar.isPrintable()) {
                    var = newVar;
                    logger.debug("Convertion an octet stream from {} to {}", octetVar, var);
                }
            }
            return var.toString();
        case SMIConstants.SYNTAX_NULL:
            return null;
        case SMIConstants.SYNTAX_OBJECT_IDENTIFIER:
            return smartPrint( (OID) var );
        case 9: // REAL
            break;
        case 10: // ENUMERATED
            break;
        case SMIConstants.SYNTAX_IPADDRESS:
            return ((IpAddress)var).getInetAddress();
        case SMIConstants.SYNTAX_COUNTER32:
        case SMIConstants.SYNTAX_GAUGE32: //SYNTAX_UNSIGNED_INTEGER32
            return var.toLong();
        case SMIConstants.SYNTAX_TIMETICKS:
            return new Double(1.0 * ((TimeTicks)var).toMilliseconds() / 1000.0);
        case SMIConstants.SYNTAX_OPAQUE:
            return resolvOpaque((Opaque) var);
        case SMIConstants.SYNTAX_COUNTER64:
            return var.toLong();
        }
        if(var instanceof UnsignedInteger32) {
            retvalue  = var.toLong();
        }
        else if(var instanceof Integer32)
            retvalue  = var.toInt();
        else if(var instanceof Counter64)
            retvalue  = var.toLong();
        else {
            logger.warn("Unknown syntax: " + var.getSyntaxString());
        }
        return retvalue;
    }

    private Object resolvOpaque(Opaque var) {

        //If not resolved, we will return the data as an array of bytes
        Object value = var.getValue();

        try {
            byte[] bytesArray = var.getValue();
            ByteBuffer bais = ByteBuffer.wrap(bytesArray);
            BERInputStream beris = new BERInputStream(bais);
            byte t1 = bais.get();
            byte t2 = bais.get();
            int l = BER.decodeLength(beris);
            if(t1 == TAG1) {
                if(t2 == TAG_FLOAT && l == 4)
                    value = new Float(bais.getFloat());
                else if(t2 == TAG_DOUBLE && l == 8)
                    value = new Double(bais.getDouble());
            }
        } catch (IOException e) {
            logger.error(var.toString());
        }
        return value;
    }

    private void loadOids(InputStream is) throws IOException {
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
