package loghub.zmq;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.ClosedSelectorException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZConfig;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import fr.loghub.naclprovider.NaclCertificate;
import fr.loghub.naclprovider.NaclPrivateKeySpec;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.zmq.ZMQHelper.Method;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;

public class ZMQSocketFactory implements AutoCloseable {

    private static final AtomicInteger MONITOR_COUNT = new AtomicInteger();
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
    private final ZapService zapService;
    @Getter
    private final ZPoller monitorPoller;
    private final Thread monitorThread;

    private final Map<String, ZConfig> publicKeys;
    private UncaughtExceptionHandler delegatedExceptionHandler = this::defaultUncaughtExceptionHandler;

    public static ZMQSocketFactoryBuilder builder() {
        return new ZMQSocketFactoryBuilder()
                       .numSocket(1)
                       .linger(DEFAULTLINGER)
                       .withZap(true);
    }

    public static ZMQSocketFactory getFactory(Map<String, Object> properties) {
        ZMQSocketFactoryBuilder builder = ZMQSocketFactory.builder();
        if (properties.containsKey("keystore")) {
            builder.zmqKeyStore = Paths.get((String)properties.remove("keystore"));
        }
        if (properties.containsKey("certsDirectory")) {
            builder.zmqCertsDir = Paths.get((String)properties.remove("certsDirectory"));
        }
        if (properties.containsKey("withZap")) {
            builder.withZap = (Boolean) properties.remove("withZap");
        }
        if (properties.containsKey("numSocket")) {
            builder.numSocket = (Integer) properties.remove("numSocket");
        }
        if (properties.containsKey("linger")) {
            builder.linger = (Integer) properties.remove("linger");
        }
        return builder.build();
    }

    public ZMQSocketFactory() {
        this(builder());
    }

    public ZMQSocketFactory(int numSocket, Path zmqKeyStore) {
        this(builder().numSocket(numSocket).zmqKeyStore(zmqKeyStore));
    }

    public ZMQSocketFactory(Path zmqKeyStore) {
        this(builder().zmqKeyStore(zmqKeyStore));
    }

    public ZMQSocketFactory(ZMQSocketFactoryBuilder builder) {
        this(builder.numSocket, builder.zmqKeyStore, builder.withZap, builder.zmqCertsDir, builder.linger);
    }

    @Builder
    public ZMQSocketFactory(int numSocket, Path zmqKeyStore, boolean withZap, Path zmqCertsDir, int linger) {
        logger.debug("New ZMQ socket factory instance");
        context = new ZContext(numSocket);
        context.setLinger(linger);
        context.setNotificationExceptionHandler((t, e) -> {
            logger.warn("Handler exception in poller: {}", () -> Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        });
        context.setUncaughtExceptionHandler(this::delegateExceptionHandler);

        terminator = ThreadBuilder.get()
                                  .setDaemon(true)
                                  .setName("ZMQTerminator")
                                  .setTask(() -> {
                                      logger.debug("starting shutdown hook for ZMQ");
                                      context.close();
                                  }).setShutdownHook(true).build();

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
        CountDownLatch monitorLatch = new CountDownLatch(1);
        monitorThread = ThreadBuilder.get()
                                     .setName("zmqmonitor" + MONITOR_COUNT.incrementAndGet())
                                     .setTask(() -> monitorLoop(monitorLatch))
                                     .build(true);
        try {
            monitorLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (zmqCertsDir != null) {
            Map<String, ZConfig> buildingPublicKeys = new HashMap<>();
            try (Stream<Path> certs = Files.find(zmqCertsDir, 10, (p, a)-> Files.isRegularFile(p) && p.endsWith(".zpl"))) {
                for (Path tryCert: (Iterable<Path>) certs::iterator) {
                    ZConfig zconf = ZConfig.load(tryCert.toString());
                    String publicKey = zconf.getValue("curve/public-key");
                    if (publicKey.length() == 32) { // we want to store the public-key as Z85-String
                        publicKey = ZMQ.Curve.z85Encode(publicKey.getBytes(ZMQ.CHARSET));
                        buildingPublicKeys.put(publicKey, zconf);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            publicKeys = Collections.unmodifiableMap(buildingPublicKeys);
        } else {
            publicKeys = Collections.emptyMap();
        }
        if (withZap) {
            zapService = new ZapService(this);
        } else {
            zapService = null;
        }
    }

    private void monitorLoop(CountDownLatch monitorLatch) {
        monitorLatch.countDown();
        monitorLatch = null;
        while (! context.isClosed()) {
            try {
                if (monitorPoller.registered() == 0) {
                    Thread.sleep(Long.MAX_VALUE);
                } else {
                    monitorPoller.poll(-1);
                }
            } catch (ClosedSelectorException e) {
                // The selector was closed so running is finished
                break;
            } catch (InterruptedException e) {
                // If real interruption, context will be closed
                // If not, it was a poll refresh
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
            ks.load(null);

            KeyPairGenerator kpg = KeyPairGenerator.getInstance(NACLKEYFACTORY.getAlgorithm());
            kpg.initialize(256);
            KeyPair kp = kpg.generateKeyPair();
            NaclCertificate certificate = new NaclCertificate(kp.getPublic());
            ks.setKeyEntry(KEYNAME, kp.getPrivate(), emptypass, new Certificate[] {certificate});
            try (FileOutputStream ksstream = new FileOutputStream(zmqKeyStore.toFile())) {
                ks.store(ksstream, emptypass);
            }

            Path publicKeyPath = Paths.get(zmqKeyStore.toString().replaceAll("\\.[a-z]+$", ".pub"));
            try (PrintWriter writer = new PrintWriter(publicKeyPath.toFile(), StandardCharsets.UTF_8)) {
                writer.print(ZMQHelper.makeServerIdentity(certificate));
            }
            Path publicKeyZplPath = Paths.get(zmqKeyStore.toString().replaceAll("\\.[a-z]+$", ".zpl"));
            try (PrintWriter writer = new PrintWriter(publicKeyZplPath.toFile(), StandardCharsets.UTF_8)) {
                writer.println("curve");
                writer.print("    public-key = \"");
                writer.print(ZMQHelper.makeServerIdentityZ85(certificate));
                writer.println("\"");
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

    public ZapDomainHandler getCertDirectoryFilter() {
        return r -> {
            String publicKey = ZMQ.Curve.z85Encode(r.getClientKey());
            if (publicKeys.containsKey(publicKey)) {
                Map<String, String> metadata = publicKeys.get(publicKey).getValues();
                String userId = metadata.remove(Metadata.USER_ID);
                r.setIdentity(userId != null ? userId : publicKey, metadata);
                return true;
            } else {
                return false;
            }
        };
    }

    public synchronized void close() throws ZMQCheckedException {
        try {
            if (! context.isClosed()) {
                Runtime.getRuntime().removeShutdownHook(terminator);
                try {
                    monitorPoller.close();
                } catch (IOException e) {
                    logger.warn("Unable to close monitor poller: {}", Helpers.resolveThrowableException(e));
                    logger.debug(e);
                }
                Optional.ofNullable(zapService).ifPresent(ZapService::close);
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


    // Delegate the exception handling, as it must be set when the context is started,
    // but we will modify it.
    private void delegateExceptionHandler(Thread thread, Throwable throwable) {
        delegatedExceptionHandler.uncaughtException(thread, throwable);
    }

    // The default exception handler will just log exceptions
    private void defaultUncaughtExceptionHandler(Thread thread, Throwable throwable) {
        logger.error("Unhandled exception", throwable);
    }

    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        delegatedExceptionHandler = handler;
    }

    @Accessors(chain=true)
    public class SocketBuilder {
        private final Method method;
        private final SocketType type;
        private final String endpoint;
        @Setter
        private PrivateKeyEntry curveKeys = ZMQSocketFactory.this.keyEntry;
        @Setter
        private Certificate serverPublicKey = null;
        @Setter
        private String zapDomain = null;
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
        @Setter
        private boolean keepAlive = true;
        private int tcpKeepAliveCnt = -1;
        private int tcpKeepAliveIdle = -1;
        private int tcpKeepAliveIntvl = -1;

        private Logger socketLogger = null;

        private SocketBuilder(Method method, SocketType type, String endpoint) {
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
        public SocketBuilder setCurveServer() {
            this.security = Mechanisms.CURVE;
            this.serverPublicKey = null;
            return this;
        }
        public SocketBuilder setCurveClient(Certificate serverPublicKey) {
            this.security = Mechanisms.CURVE;
            this.serverPublicKey = serverPublicKey;
            return this;
        }
        public SocketBuilder setLoggerMonitor(String name, Logger socketLogger) {
            this.monitor = String.format("inproc://monitor/%s/%s", name, UUID.randomUUID());
            this.socketLogger = socketLogger;
            return this;
        }

        public SocketBuilder setKeepAliveSettings(int tcpKeepAliveCnt, int tcpKeepAliveIdle, int tcpKeepAliveIntvl) {
            this.keepAlive = true;
            this.tcpKeepAliveCnt = tcpKeepAliveCnt;
            this.tcpKeepAliveIdle = tcpKeepAliveIdle;
            this.tcpKeepAliveIntvl = tcpKeepAliveIntvl;
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
                if (monitor != null) {
                    socket.monitor(monitor, ZMQ.EVENT_ALL);
                    if (socketLogger != null) {
                        Socket socketMonitor = ZMQSocketFactory.this.getBuilder(Method.CONNECT, SocketType.PAIR, monitor).build();
                        monitorPoller.register(socketMonitor, new EventLogger(monitorPoller, url, logger), ZPoller.IN | ZPoller.ERR);
                        // Will exit from poll, and refresh the monitored sockets
                        monitorThread.interrupt();
                    }
                }
                if (zapDomain != null) {
                    socket.setZAPDomain(zapDomain);
                }
                socket.setTCPKeepAlive(this.keepAlive ? 1: 0);
                if (keepAlive) {
                    socket.setTCPKeepAliveCount(tcpKeepAliveCnt);
                    socket.setTCPKeepAliveIdle(tcpKeepAliveIdle);
                    socket.setTCPKeepAliveInterval(tcpKeepAliveIntvl);
                }
                socket.setSelfAddressPropertyName("X-Self-Address");
                switch (security) {
                case CURVE:
                    setCurveSecuritySettings(socket);
                    break;
                case NULL:
                    break;
                default:
                    throw new IllegalArgumentException("Security "+ security + " not managed");
                }
                if (type == SocketType.SUB && topic != null) {
                    socket.subscribe(topic);
                }
                socket.setImmediate(immediate);
                if (! method.act(socket, endpoint)) {
                    throw new IllegalStateException("Failed to act on " + url);
                }
                logger.trace("New socket: {}={} in {}", url, socket, context);
                return socket;
            } catch (UncheckedZMQException e) {
                if (socket != null) {
                    socket.close();
                }
                throw new ZMQCheckedException(e);
            }
        }
        private void setCurveSecuritySettings(Socket socket) throws ZMQCheckedException {
            ZMQCheckedException.checkOption(socket.setCurveServer(serverPublicKey == null), socket);
            if (curveKeys != null) {
                try {
                    NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(curveKeys.getCertificate().getPublicKey(), NaclPublicKeySpec.class);
                    ZMQCheckedException.checkOption(socket.setCurvePublicKey(pubkey.getBytes()), socket);
                    NaclPrivateKeySpec privateKey = NACLKEYFACTORY.getKeySpec(curveKeys.getPrivateKey(), NaclPrivateKeySpec.class);
                    ZMQCheckedException.checkOption(socket.setCurveSecretKey(privateKey.getBytes()), socket);
                } catch (InvalidKeySpecException e) {
                    throw new IllegalArgumentException("Invalid curve keys pair");
                }
                if (serverPublicKey != null) {
                    try {
                        NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(serverPublicKey.getPublicKey(), NaclPublicKeySpec.class);
                        ZMQCheckedException.checkOption(socket.setCurveServerKey(pubkey.getBytes()), socket);
                    } catch (InvalidKeySpecException e) {
                        throw new IllegalArgumentException("Invalid remote public curve key");
                    }
                }
            } else {
                throw new IllegalArgumentException("Curve security requested, but no keys given");
            }
        }
    }

    public SocketBuilder getBuilder(Method method, SocketType pub,
                                    String endpoint) {
        return new SocketBuilder(method, pub, endpoint);
    }

}
