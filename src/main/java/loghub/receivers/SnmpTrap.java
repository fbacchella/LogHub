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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.asn1.BER;
import org.snmp4j.asn1.BERInputStream;
import org.snmp4j.log.Log4jLogFactory;
import org.snmp4j.log.LogFactory;
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
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import fr.jrds.SmiExtensions.MibTree;
import fr.jrds.SmiExtensions.objects.ObjectInfos;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"protocol", "port", "listen"})
public class SnmpTrap extends Receiver implements CommandResponder {

    static {
        LogFactory.setLogFactory(new Log4jLogFactory());
    }

    private static final Logger logger = LogManager.getLogger();

    static final private byte TAG1 = (byte) 0x9f;
    static final private byte TAG_FLOAT = (byte) 0x78;
    static final private byte TAG_DOUBLE = (byte) 0x79;

    static private Snmp snmp;
    static private boolean reconfigured = false;

    private ThreadPool threadPool;
    private String protocol = "udp";
    private int port = 162;
    private String listen = "0.0.0.0";
    private MibTree mibtree = null;

    public SnmpTrap(BlockingQueue<Event> outQueue, Pipeline processors) {
        super(outQueue, processors);
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
        if(mibtree == null) {
            mibtree = new MibTree();
        }

        if(! reconfigured && properties.containsKey("oidfile")) {
            reconfigured = true;
            String oidfile = null;
            try {
                oidfile = (String) properties.get("oidfile");
                InputStream is = new FileInputStream(oidfile);
                mibtree.load(is);
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
        Event event = emptyEvent();
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

    public List<String> smartPrint(OID oid) {
        return Arrays.stream(mibtree.parseIndexOID(oid)).map( i-> i.toString()).collect(Collectors.toList());
    }

    private void smartPut(Event e, OID oid, Object value) {
        ObjectInfos found = mibtree.searchInfos(oid);
        if(found == null) {
            e.put(oid.toDottedString(), value);
        }
        else {
            ObjectInfos parent = mibtree.getParent(found.getOidElements());
            if(parent != null && parent.isIndex()) {
                Map<String, Object> valueMap = new HashMap<>(3);
                Object[] resolved = mibtree.parseIndexOID(oid);
                valueMap.put("index", Arrays.copyOfRange(resolved, 1, resolved.length));
                valueMap.put("value", value);
                e.put(found.getName(), valueMap);
            } else {
                int oidCompare = found.compareTo(oid);
                if (oidCompare == 0 || (-oidCompare == oid.size() && oid.last() == 0)) {
                    // if found exactly or was suffixed by .0, put the value directly
                    e.put(found.getName(), value);
                } else { // found OID is shorter than request OID
                    OID foundOID = found.getOID();
                    int[] suffixes = Arrays.copyOfRange(oid.getValue(), foundOID.size(), oid.size());
                    Map<String, Object> valueMap = new HashMap<>(2);
                    // Put suboid only if it's not .0
                    valueMap.put("suboid", new OID(suffixes));
                    valueMap.put("value", value);
                    e.put(found.getName(), valueMap);
                }
            }
        }
    }

    private Object convertVar(Variable var) {
        if(var == null) {
            return null;
        }
        if(var instanceof UnsignedInteger32) {
            return var.toLong();
        }
        else if(var instanceof Integer32) {
            return var.toInt();
        }
        else if(var instanceof Counter64) {
            return var.toLong();
        }
        else {
            switch(var.getSyntax()) {
            case BER.ASN_BOOLEAN:
                return var.toInt();
            case BER.INTEGER:
                return var.toInt();
            case BER.OCTETSTRING:
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
            case BER.NULL:
                return null;
            case BER.OID:
                return smartPrint( (OID) var );
            case BER.IPADDRESS:
                return ((IpAddress)var).getInetAddress();
            case BER.COUNTER32:
            case BER.GAUGE32:
                return var.toLong();
            case BER.TIMETICKS:
                return new Double(1.0 * ((TimeTicks)var).toMilliseconds() / 1000.0);
            case BER.OPAQUE:
                return resolvOpaque((Opaque) var);
            case BER.COUNTER64:
                return var.toLong();
            default:
                logger.warn("Unknown syntax: " + var.getSyntaxString());
                return null;
            }
        }
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
