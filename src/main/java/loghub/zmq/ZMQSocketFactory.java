package loghub.zmq;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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
import fr.loghub.naclprovider.NaclProvider;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.zmq.ZMQHelper.Method;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.io.mechanism.Mechanisms;

public class ZMQSocketFactory implements AutoCloseable {

    public static final String KEYNAME = "loghubzmqpair";
    public static final int DEFAULTLINGER = 1000;

    private static final Logger logger = LogManager.getLogger();

    private final ZContext context;
    //private final ThreadLocal<ZContext> localContext;
    @Getter
    private final PrivateKeyEntry keyEntry;
    private final Thread terminator;

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
            } catch (KeyStoreException | NoSuchAlgorithmException
                            | CertificateException | InvalidKeySpecException
                            | UnrecoverableEntryException | IOException | InvalidKeyException ex) {
                throw new IllegalArgumentException("Can't load the key store", ex);
            }
        } else {
            keyEntry = null;
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
        default:
            throw new IllegalArgumentException("Unsupported key store type");
        }
        KeyStore ks = KeyStore.getInstance(keystoretype);

        char[] emptypass = new char[] {};

        if (! Files.exists(zmqKeyStore)) {
            logger.debug("Creating a new keystore at {}", zmqKeyStore);
            KeyFactory kf = KeyFactory.getInstance(NaclProvider.NAME);
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
                context.close();
                logger.debug("Global ZMQ context {} terminated", context);
            }
        } catch (UncheckedZMQException e) {
            throw new ZMQCheckedException(e);
        }
    }

    public ZMonitor getZMonitor(Socket s) {
        return new ZMonitor(context, s);
    }

    public ZPoller getZPoller() {
        return new ZPoller(context);
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
        private SocketBuilder(Method method, SocketType type, String endpoint) {
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
        public SocketBuilder setCurveKeys(PrivateKeyEntry pke) throws ZMQCheckedException {
            this.pke = pke;
            return this;
        }
        public SocketBuilder setCurveClient(Certificate serverPublicKey) throws ZMQCheckedException {
            this.serverPublicKey = serverPublicKey;
            return this;
        }
        public SocketBuilder setCurveServer() {
            this.serverPublicKey = null;
            return this;
        }
        public Socket build() throws ZMQCheckedException {
            try {
                Socket socket = context.createSocket(type);
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
                }
                switch (security) {
                case CURVE:
                    if (pke != null) {
                        try {
                            NaclPublicKeySpec pubkey = ZMQHelper.NACLKEYFACTORY.getKeySpec(pke.getCertificate().getPublicKey(), NaclPublicKeySpec.class);
                            ZMQCheckedException.checkOption(socket.setCurvePublicKey(pubkey.getBytes()), socket);
                            NaclPrivateKeySpec privateKey = ZMQHelper.NACLKEYFACTORY.getKeySpec(pke.getPrivateKey(), NaclPrivateKeySpec.class);
                            ZMQCheckedException.checkOption(socket.setCurveSecretKey(privateKey.getBytes()), socket);
                        } catch (InvalidKeySpecException e) {
                            throw new IllegalArgumentException("Invalide curve keys pair");
                        }
 
                        ZMQCheckedException.checkOption(socket.setCurveServer(serverPublicKey == null), socket);
                        if (serverPublicKey != null) {
                            try {
                                NaclPublicKeySpec pubkey = ZMQHelper.NACLKEYFACTORY.getKeySpec(serverPublicKey.getPublicKey(), NaclPublicKeySpec.class);
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
                throw new ZMQCheckedException(e);
            }
        }
    }

    public SocketBuilder getBuilder(Method method, SocketType pub,
                                    String endpoint) {
        return new SocketBuilder(method, pub, endpoint);
    }

}
