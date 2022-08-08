package loghub.receivers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.TransportStateReference;
import org.snmp4j.asn1.BER;
import org.snmp4j.asn1.BERInputStream;
import org.snmp4j.log.LogFactory;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Opaque;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.TransportIpAddress;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import fr.jrds.snmpcodec.OIDFormatter;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.IpConnectionContext;
import loghub.configuration.Properties;
import loghub.metrics.Stats;
import loghub.snmp.Log4j2LogFactory;
import lombok.Getter;
import lombok.Setter;

@SelfDecoder
@BuilderClass(SnmpTrap.Builder.class)
public class SnmpTrap extends Receiver<SnmpTrap, SnmpTrap.Builder> implements CommandResponder {

    static {
        LogFactory.setLogFactory(new Log4j2LogFactory());
    }

    private enum GENERICTRAP {
        coldStart,
        warmStart,
        linkDown,
        linkUp,
        authenticationFailure,
        egpNeighborLoss,
        enterpriseSpecific
    }

    enum PROTOCOL {
        udp,
        tcp,
    }

    private static final byte TAG1 = (byte) 0x9f;
    private static final byte TAG_FLOAT = (byte) 0x78;
    private static final byte TAG_DOUBLE = (byte) 0x79;

    public static class Builder extends Receiver.Builder<SnmpTrap, SnmpTrap.Builder> {
        @Setter
        private PROTOCOL protocol = PROTOCOL.udp;
        @Setter
        private int port = 162;
        @Setter
        private String listen = "0.0.0.0";
        @Setter
        private int worker = 1;
        @Override
        public SnmpTrap build() {
            return new SnmpTrap(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static OIDFormatter formatter = null;

    @Getter
    private final PROTOCOL protocol;
    @Getter
    private final int port;
    @Getter
    private final String listen;

    private Snmp snmp;
    private final ThreadPool threadPool;

    protected SnmpTrap(Builder builder) {
        super(builder);
        this.protocol = builder.protocol;
        this.port = builder.port;
        this.listen = builder.listen;
        this.threadPool = ThreadPool.create("Trap", builder.worker);
    }

    private static synchronized void reconfigure(Logger logger, Properties properties) {
        if (formatter == null && properties.containsKey("mibdirs")) {
            try {
                String[] mibdirs = Arrays.stream((Object[]) properties.get("mibdirs")).map(Object::toString).toArray(String[]::new);
                formatter = OIDFormatter.register(mibdirs);
            } catch (ClassCastException e) {
                logger.error("mibdirs property is not an array");
                logger.catching(Level.DEBUG, e.getCause());
            }
        } else if (formatter == null) {
            formatter = OIDFormatter.register();
        }
    }

    public static synchronized void resetMibDirs() {
        formatter = null;
    }

    @Override
    public boolean configure(Properties properties) {
        reconfigure(logger, properties);
        MultiThreadedMessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(threadPool,
                                                                                       new MessageDispatcherImpl());
        dispatcher.addCommandResponder(this);
        dispatcher.addMessageProcessingModel(new MPv1());
        dispatcher.addMessageProcessingModel(new MPv2c());
        Address listenAddress = GenericAddress.parse(protocol + ":" + listen + "/" + port);
        TransportMapping<?> transport;
        try {
            switch (protocol) {
                case udp:
                    transport = new DefaultUdpTransportMapping((UdpAddress) listenAddress);
                    break;
                case tcp:
                    transport = new DefaultTcpTransportMapping((TcpAddress) listenAddress);
                    break;
                default:
                    // unreachable code
                    return false;
            }
        } catch (IOException e) {
            logger.error("can't bind to {}: {}", listenAddress, e.getMessage());
            return false;
        }
        transport.addTransportListener(this::doStats);
        snmp = new Snmp(dispatcher, transport);
        try {
            snmp.listen();
        } catch (IOException e) {
            logger.error("can't listen: {}", e.getMessage());
            return false;
        }
        return super.configure(properties);
    }

    private <A extends Address> void doStats(TransportMapping<? super A> transportMapping, A a,
            ByteBuffer byteBuffer, TransportStateReference transportStateReference) {
        Stats.newReceivedMessage(SnmpTrap.this, byteBuffer.remaining());
    }

    @Override
    public void run() {
        try {
            synchronized(snmp) {
                snmp.wait();
            }
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        try {
            snmp.close();
        } catch (IOException e1) {
            logger.error("Failure on snmp close: {}", () -> e1);
            logger.catching(e1);
        }
        super.close();
    }

    private InetSocketAddress getSA(TransportIpAddress tia) {
        return new InetSocketAddress(tia.getInetAddress(), tia.getPort());
    }

    @Override
    public void processPdu(CommandResponderEvent trap) {
        try {
            PDU pdu = trap.getPDU();
            Address localaddr = trap.getTransportMapping().getListenAddress();
            Address remoteaddr = trap.getPeerAddress();
            ConnectionContext<?> ctx = ConnectionContext.EMPTY;
            if (localaddr instanceof TransportIpAddress && remoteaddr instanceof TransportIpAddress ) {
                InetSocketAddress localinetaddr = getSA((TransportIpAddress) localaddr);
                InetSocketAddress remoteinetaddr = getSA((TransportIpAddress) remoteaddr);
                ctx = new IpConnectionContext(localinetaddr, remoteinetaddr, null);
            }
            Map<String, Object> eventMap = new HashMap<>();
            if (pdu instanceof PDUv1) {
                PDUv1 pduv1 = (PDUv1) pdu;
                String enterprise = (String) convertVar(pduv1.getEnterprise());
                eventMap.put("enterprise", enterprise);
                eventMap.put("agent_addr", pduv1.getAgentAddress().getInetAddress());
                if (pduv1.getGenericTrap() != PDUv1.ENTERPRISE_SPECIFIC) {
                    eventMap.put("generic_trap", GENERICTRAP.values()[pduv1.getGenericTrap()].toString());
                } else {
                    String resolved = formatter.format(pduv1.getEnterprise(), new Integer32(pduv1.getSpecificTrap()), true);
                    eventMap.put("specific_trap", resolved);
                }
                eventMap.put("time_stamp", 1.0 * pduv1.getTimestamp() / 100.0);
            }
            for (VariableBinding i: pdu.getVariableBindings()) {
                OID vbOID = i.getOid();
                Object value = convertVar(i.getVariable());
                smartPut(eventMap, vbOID, value);
            }
            // If SNMPv2c, try to save the community as a principal
            if (trap.getMessageProcessingModel() == MessageProcessingModel.MPv2c) {
                Optional.ofNullable(trap.getSecurityName())
                .filter( i -> i.length > 0)
                .map(i -> new String(i, StandardCharsets.UTF_8))
                .map(n -> (Principal) () -> n)
                .ifPresent(ctx::setPrincipal);
            }
            send(mapToEvent(ctx, eventMap));
        } catch (RuntimeException ex) {
            Stats.newUnhandledException(this, ex);
        } finally {
            trap.setProcessed(true);
        }
    }

    private void smartPut(Map<String, Object> e, OID oid, Object value) {
        Map<String, Object> oidindex = formatter.store.parseIndexOID(oid.getValue());
        if (oidindex.size() == 0) {
            e.put(oid.format(), value);
        } else if (oidindex.size() == 1) {
            Object indexvalue = oidindex.values().stream().findFirst().orElse(null);
            // it's an array, so it's a unresolved index
            if (indexvalue != null && indexvalue.getClass().isArray()) {
                Map<String, Object> valueMap = new HashMap<>(2);
                valueMap.put("index", indexvalue);
                valueMap.put("value", value);
                e.put(oid.format(), valueMap);
            } else {
                e.put(oid.format(), value);
            }
        } else {
            String tableName = oidindex.keySet().stream().findFirst().orElse(null);
            Object rowName = oidindex.remove(tableName);
            Map<String, Object> valueMap = new HashMap<>(2);
            valueMap.put("index", oidindex);
            valueMap.put("value", value);
            e.put(rowName.toString(), valueMap);
        }
    }

    private Object convertVar(Variable variable) {
        if (variable == null) {
            return null;
        } else {
            switch (variable.getSyntax()) {
            case BER.ASN_BOOLEAN:
                return variable.toInt();
            case BER.INTEGER:
                return variable.toInt();
            case BER.OCTETSTRING:
                Variable stringVar = variable;
                //It might be a C string, try to remove the last 0
                //But only if the new string is printable
                OctetString octetVar = (OctetString) variable;
                int length = octetVar.length();
                if (length > 1 && octetVar.get(length - 1) == 0) {
                    OctetString newVar = octetVar.substring(0, length - 1);
                    if (newVar.isPrintable()) {
                        stringVar = newVar;
                        logger.debug("Convertion an octet stream from {} to {}", octetVar, stringVar);
                    }
                }
                return stringVar.toString();
            case BER.NULL:
                return null;
            case BER.OID: {
                OID oid = (OID) variable;
                Map<String, Object> parsed = formatter.store.parseIndexOID(oid.getValue());
                // If an empty map was return or a single entry map, it's not an table entry, just format the OID
                return parsed.size() <= 1 ? oid.format() : parsed;
            }
            case BER.IPADDRESS:
                return ((IpAddress)variable).getInetAddress();
            case BER.COUNTER32:
            case BER.GAUGE32:
                return variable.toLong();
            case BER.TIMETICKS:
                return 1.0 * ((TimeTicks)variable).toMilliseconds() / 1000.0;
            case BER.OPAQUE:
                return resolvOpaque((Opaque) variable);
            case BER.COUNTER64:
                return variable.toLong();
            default:
                if (variable instanceof UnsignedInteger32) {
                    return variable.toLong();
                } else if (variable instanceof Integer32) {
                    return variable.toInt();
                } else if (variable instanceof Counter64) {
                    return variable.toLong();
                } else {
                    logger.warn("Unknown syntax: {}", variable::getSyntaxString);
                    return null;
                }
            }
        }
    }

    private Object resolvOpaque(Opaque variable) {
        //If not resolved, we will return the data as an array of bytes
        Object value = variable.getValue();
        try {
            byte[] bytesArray = variable.getValue();
            ByteBuffer bais = ByteBuffer.wrap(bytesArray);
            BERInputStream beris = new BERInputStream(bais);
            byte t1 = bais.get();
            byte t2 = bais.get();
            int l = BER.decodeLength(beris);
            if (t1 == TAG1) {
                if (t2 == TAG_FLOAT && l == 4) {
                    value = bais.getFloat();
                } else if (t2 == TAG_DOUBLE && l == 8){
                    value = bais.getDouble();
                }
            }
        } catch (IOException e) {
            logger.error(variable.toString());
        }
        return value;
    }

    @Override
    public String getReceiverName() {
        return "SnmpTrap/" + Helpers.ListenString(listen) + "/" + getPort();
    }

}
