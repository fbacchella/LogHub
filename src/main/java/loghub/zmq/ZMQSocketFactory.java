package loghub.zmq;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMonitor;
import org.zeromq.ZPoller;

import fr.loghub.naclprovider.NaclCertificate;
import fr.loghub.naclprovider.NaclPrivateKeySpec;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.zmq.ZMQHelper.Method;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.ZError;
import zmq.io.mechanism.Mechanisms;

import static org.zeromq.ZMQ.DONTWAIT;

public class ZMQSocketFactory implements AutoCloseable {

    // Needed to ensure the NaCl Provided is loaded
    private static final KeyFactory NACLKEYFACTORY = ZMQHelper.NACLKEYFACTORY;

    public static final String KEYNAME = "loghubzmqpair";
    public static final int DEFAULTLINGER = 1000;

    private static final Logger logger = LogManager.getLogger();

    private final ZContext context;

    @Getter
    private final PrivateKeyEntry keyEntry;
    private final Thread terminator;
    @Getter
    private final ZPoller monitorPoller;
    private final Thread monitorThread;

    public static ZMQSocketFactory getFactory(Map<String, Object> properties) {
        int numSocket;
        Path zmqKeyStore;
        int linger;
        if (properties.containsKey("keystore")) {
            zmqKeyStore = Paths.get((String)properties.remove("keystore"));
        } else {
            zmqKeyStore = null;
        }
        if (properties.containsKey("numSocket")) {
            numSocket = (Integer) properties.remove("numSocket");
        } else {
            numSocket = 1;
        }
        if (properties.containsKey("linger")) {
            linger = (Integer) properties.remove("linger");
        } else {
            linger = DEFAULTLINGER;
        }
        return new ZMQSocketFactory(numSocket, zmqKeyStore, linger);
    }

    private class EventLogger implements ZPoller.EventsHandler {

        private final Logger eventLogger;
        private final String socketUrl;

        private EventLogger(String socketUrl, Logger eventLogger) {
            this.eventLogger = eventLogger;
            this.socketUrl = socketUrl;
        }

        @Override
        public boolean events(Socket socket, int events) {
            while (true) {
                ZMQ.Event e = ZMQ.Event.recv(socket, DONTWAIT);
                if (e == null) {
                    break;
                }
                ZMonitor.Event evType = ZMonitor.Event.findByCode(e.getEvent());
                if (e.isError() && evType == ZMonitor.Event.HANDSHAKE_FAILED_PROTOCOL) {
                    eventLogger.error("Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
                } else if (evType == ZMonitor.Event.MONITOR_STOPPED) {
                    eventLogger.trace("Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
                } else if (e.isError()) {
                    eventLogger.warn("Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
                } else if (e.isWarn()) {
                    eventLogger.warn("Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
                } else {
                    eventLogger.debug("Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
                }
                if (evType == ZMonitor.Event.MONITOR_STOPPED) {
                    ZMQSocketFactory.this.monitorPoller.unregister(socket);
                    socket.close();
                    break;
                }
            }
            return true;
        }

        private String resolvedEvent(ZMonitor.Event evType, ZMQ.Event ev) {
            switch (evType) {
            case HANDSHAKE_PROTOCOL: {
                Integer version = ev.resolveValue();
                return String.format("Handshake protocol, version %s", version);
            }
            case MONITOR_STOPPED:
                return "Monitor stopped";
            case CONNECT_DELAYED:
                return "Connect delayed";
            case LISTENING: {
                ServerSocketChannel ch = ev.resolveValue();
                try {
                    return String.format("Listening on %s", ch.getLocalAddress());
                } catch (IOException e) {
                    return String.format("Listening on %s", ch);
                }
            }
            case CONNECTED: {
                SocketChannel ch = ev.resolveValue();
                try {
                    return String.format("Connect from %s to %s", ch.getLocalAddress(), ch.getRemoteAddress());
                } catch (IOException e) {
                    return String.format("Connected channel %s", ch);
                }
            }
            case DISCONNECTED:
                return "Disconnected";
            case CLOSED:
                return "Closed";
            case ACCEPTED: {
                SocketChannel ch = ev.resolveValue();
                try {
                    return String.format("Accepted on %s from %s", ch.getLocalAddress(), ch.getRemoteAddress());
                } catch (IOException e) {
                    return String.format("Accepted channel %s", ch);
                }
            }
            default:
                // Nothing special to do
            }
            Object value = ev.resolveValue();
            return String.format("%s%s%s", evType, value != null ? ": " : "", value != null ? value : "");
        }

        @Override
        public boolean events(SelectableChannel channel, int events) {
            // Should never be reached
            throw new IllegalStateException("A channel was not expected: " + channel);
        }
    }

    public ZMQSocketFactory() {
        this(1, null, DEFAULTLINGER);
    }
    
    public ZMQSocketFactory(int numSocket) {
        this(numSocket, null, DEFAULTLINGER);
    }
    
    public ZMQSocketFactory(int numSocket, Path zmqKeyStore) {
        this(numSocket, zmqKeyStore, DEFAULTLINGER);
    }

    public ZMQSocketFactory(int numSocket, int linger) {
        this(numSocket, null, linger);
    }

    public ZMQSocketFactory(Path zmqKeyStore) {
        this(1, zmqKeyStore, DEFAULTLINGER);
    }

    public ZMQSocketFactory(int numSocket, Path zmqKeyStore, int linger) {
        logger.debug("New ZMQ socket factory instance");
        context = new ZContext(numSocket);
        context.setLinger(linger);
        context.setNotificationExceptionHandler((t, e) -> {
            logger.warn("Handler exception in poller: {}", () -> Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        });

        terminator = ThreadBuilder.get()
                                  .setDaemon(true)
                                  .setName("ZMQTerminator")
                                  .setTask(() -> {
                                      logger.debug("starting shutdown hook for ZMQ");
                                      context.close();
                                  }).setShutdownHook(false).build();
        if (zmqKeyStore != null) {
            try {
                keyEntry = checkKeyStore(zmqKeyStore);
                logger.debug("Using key store {}", zmqKeyStore);
            } catch (KeyStoreException | NoSuchAlgorithmException
                            | CertificateException | InvalidKeySpecException
                            | UnrecoverableEntryException | IOException | InvalidKeyException ex) {
                throw new IllegalArgumentException("Can't load the key store", ex);
            }
        } else {
            keyEntry = null;
        }
        monitorPoller = getZPoller();
        monitorThread = ThreadBuilder.get()
                                     .setName("zmqmonitor")
                                     .setTask(this::monitorLoop)
                                     .build(true);

    }

    private void monitorLoop() {
        int count = 0;
        while (! context.isClosed() && count >= 0) {
            try {
                if (monitorPoller.registered() == 0) {
                    Thread.sleep(Long.MAX_VALUE);
                }
                count = monitorPoller.poll(-1L);
            } catch (InterruptedException e) {
                // If real interruption, context will be closed
                // If not, it's used to refresh what to poll
            }
        }
    }

    private PrivateKeyEntry checkKeyStore(Path zmqKeyStore) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, InvalidKeySpecException, UnrecoverableEntryException, InvalidKeyException {
        String keystoretype;
        switch (Helpers.getMimeType(zmqKeyStore.toString())) {
        case "application/x-java-keystore":
            keystoretype = "JKS";
            break;
        case "application/x-java-jce-keystore":
            keystoretype = "JCEKS";
            break;
        case "application/x-java-bc-keystore":
            keystoretype = "BKS";
            break;
        case "application/x-pkcs12":
            throw new IllegalArgumentException("P12 can't handle custom certificate type");
        default:
            throw new IllegalArgumentException("Unsupported key store type");
        }
        KeyStore ks = KeyStore.getInstance(keystoretype);

        char[] emptypass = new char[] {};

        if (! Files.exists(zmqKeyStore)) {
            logger.debug("Creating a new keystore at {}", zmqKeyStore);
            KeyFactory kf = NACLKEYFACTORY;
            ks.load(null);

            KeyPairGenerator kpg = KeyPairGenerator.getInstance(kf.getAlgorithm());
            kpg.initialize(256);
            KeyPair kp = kpg.generateKeyPair();
            NaclCertificate certificate = new NaclCertificate(kp.getPublic());
            ks.setKeyEntry(KEYNAME, kp.getPrivate(), emptypass, new Certificate[] {certificate});
            try (FileOutputStream ksstream = new FileOutputStream(zmqKeyStore.toFile())) {
                ks.store(ksstream, emptypass);
            }

            Path publicKeyPath = Paths.get(zmqKeyStore.toString().replaceAll("\\.[a-z]+$", ".pub"));
            try (PrintWriter writer = new PrintWriter(publicKeyPath.toFile(), "UTF-8")) {
                writer.print(ZMQHelper.makeServerIdentity(certificate));
            }
            PrivateKey prk = kp.getPrivate();
            return new PrivateKeyEntry(prk, new NaclCertificate[] {certificate} );
        } else {
            try (FileInputStream ksstream = new FileInputStream(zmqKeyStore.toFile())) {
                ks.load(ksstream, emptypass);
            }
            KeyStore.Entry e = ks.getEntry(KEYNAME, new KeyStore.PasswordProtection(emptypass));
            if (e == null) {
                throw new IllegalArgumentException("Invalid key store, the curve key is missing");
            }
            if (e instanceof PrivateKeyEntry) {
                return (PrivateKeyEntry) e;
            } else {
                throw new IllegalArgumentException("Invalid key store, the curve key type is not a private key entry");
            }
        }
    }

    public synchronized void close() throws ZMQCheckedException {
        try {
            if (! context.isClosed()) {
                Runtime.getRuntime().removeShutdownHook(terminator);
                try {
                    monitorPoller.close();
                } catch (IOException e) {
                    throw new ZMQCheckedException(new ZError.IOException(e));
                }
                context.close();
                logger.debug("Global ZMQ context {} terminated", context);
            }
        } catch (UncheckedZMQException e) {
            throw new ZMQCheckedException(e);
        }
    }

    public ZPoller getZPoller() {
        return new ZPoller(context);
    }
    
    public void setExceptionHandler(Thread.UncaughtExceptionHandler handler) {
        context.setUncaughtExceptionHandler(handler);
    }

    @Accessors(chain=true)
    public class SocketBuilder {
        private final Method method;
        private final SocketType type;
        private final String endpoint;
        private PrivateKeyEntry pke = ZMQSocketFactory.this.keyEntry;
        private Certificate serverPublicKey = null;
        @Setter
        private byte[] topic = null;
        @Setter
        private int hwm = -1;
        @Setter
        private int timeout = -1;
        @Setter
        private int linger = -2;
        @Setter
        private boolean immediate = true;
        @Setter
        private Mechanisms security = Mechanisms.NULL;
        @Setter
        private String monitor = null;

        private Logger socketLogger = null;

        private SocketBuilder(Method method, SocketType type, String endpoint) {
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
        public SocketBuilder setCurveKeys(PrivateKeyEntry pke) {
            this.pke = pke;
            return this;
        }
        public SocketBuilder setCurveClient(Certificate serverPublicKey) {
            this.serverPublicKey = serverPublicKey;
            return this;
        }
        public SocketBuilder setCurveServer() {
            this.serverPublicKey = null;
            return this;
        }
        public SocketBuilder setLoggerMonitor(String name, Logger socketLogger) {
            this.monitor = String.format("inproc://monitor/%s/%s", name, UUID.randomUUID());
            this.socketLogger = socketLogger;
            return this;
        }
        public Socket build() throws ZMQCheckedException {
            Socket socket = null;
            try {
                socket = context.createSocket(type);
                String url = endpoint + ":" + type.toString() + ":" + method.getSymbol();
                socket.setIdentity(url.getBytes(StandardCharsets.UTF_8));
                if (hwm >= 0) {
                    socket.setRcvHWM(hwm);
                    socket.setSndHWM(hwm);
                }
                if (timeout >= 0) {
                    socket.setSendTimeOut(timeout);
                    socket.setReceiveTimeOut(timeout);
                }
                if (linger != -2) {
                    socket.setLinger(linger);
                }
                if (monitor != null ) {
                    socket.monitor(monitor, ZMQ.EVENT_ALL);
                    if (socketLogger != null) {
                        Socket socketMonitor = ZMQSocketFactory.this.getBuilder(Method.CONNECT, SocketType.PAIR, monitor).build();
                        monitorPoller.register(socketMonitor, new EventLogger(url, logger), ZPoller.IN | ZPoller.ERR);
                        // Will exit from poll, and refresh the monitored sockets
                        monitorThread.interrupt();
                    }
                }
                socket.setTCPKeepAlive(1);
                socket.setTCPKeepAliveCount(3);
                socket.setTCPKeepAliveIdle(60);
                socket.setTCPKeepAliveInterval(10);
                switch (security) {
                case CURVE:
                    if (pke != null) {
                        try {
                            NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(pke.getCertificate().getPublicKey(), NaclPublicKeySpec.class);
                            ZMQCheckedException.checkOption(socket.setCurvePublicKey(pubkey.getBytes()), socket);
                            NaclPrivateKeySpec privateKey = NACLKEYFACTORY.getKeySpec(pke.getPrivateKey(), NaclPrivateKeySpec.class);
                            ZMQCheckedException.checkOption(socket.setCurveSecretKey(privateKey.getBytes()), socket);
                        } catch (InvalidKeySpecException e) {
                            throw new IllegalArgumentException("Invalide curve keys pair");
                        }
 
                        ZMQCheckedException.checkOption(socket.setCurveServer(serverPublicKey == null), socket);
                        if (serverPublicKey != null) {
                            try {
                                NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(serverPublicKey.getPublicKey(), NaclPublicKeySpec.class);
                                ZMQCheckedException.checkOption(socket.setCurveServerKey(pubkey.getBytes()), socket);
                            } catch (InvalidKeySpecException e) {
                                throw new IllegalArgumentException("Invalide remote public curve key");
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("Curve security requested, but no keys given");
                    }
                    break;
                case NULL:
                    break;
                default:
                    throw new IllegalArgumentException("Security "+ security + "not managed");
                }
                if (type == SocketType.SUB && topic != null) {
                    socket.subscribe(topic);
                }
                socket.setImmediate(immediate);
                if ( ! method.act(socket, endpoint)) {
                    throw new IllegalStateException("Failed to act on " + url);
                }
                logger.trace("new socket: {}={} in {}", url, socket, context);
                return socket;
            } catch (UncheckedZMQException e) {
                if (socket != null) {
                    socket.close();
                }
                throw new ZMQCheckedException(e);
            }
        }
    }

    public SocketBuilder getBuilder(Method method, SocketType pub,
                                    String endpoint) {
        return new SocketBuilder(method, pub, endpoint);
    }

}
