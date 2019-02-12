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
import java.security.PublicKey;
import java.security.Security;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import fr.loghub.naclprovider.NaclCertificate;
import fr.loghub.naclprovider.NaclPrivateKeySpec;
import fr.loghub.naclprovider.NaclProvider;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.zmq.ZMQHelper.Method;
import zmq.Options;
import zmq.socket.Sockets;

public class SmartContext {

    public static final String CURVEPREFIX="Curve";

    private static final Logger logger = LogManager.getLogger();

    private static SmartContext instance = null;
    private final ZContext context;
    private final ThreadLocal<ZContext> localContext;
    @SuppressWarnings("unused")
    private final ZContext firstContext;
    private volatile boolean running = true;
    private byte[] privateKey = null;
    private byte[] publicKey = null;
    private final AtomicInteger activeContext = new AtomicInteger();
    private final Thread terminator;

    // Load the nacl security handler 
    static {
        Security.insertProviderAt(new NaclProvider(), Security.getProviders().length + 1);
    }

    public static synchronized SmartContext build(Map<Object, Object> collect) {
        int numSocket;
        Path zmqKeyStore;
        if (collect.containsKey("keystore")) {
            zmqKeyStore = Paths.get((String)collect.remove("keystore"));
        } else {
            zmqKeyStore = null;
        }
        if (collect.containsKey("numSocket")) {
            numSocket = (Integer) collect.remove("numSocket");
        } else {
            numSocket = 1;
        }
        return getContext(numSocket, zmqKeyStore);
    }

    public static synchronized SmartContext getContext(Path zmqKeyStore) {
        return getContext(1, zmqKeyStore);
    }

    public static synchronized SmartContext getContext() {
        return getContext(1, null);
    }

    public static synchronized SmartContext getContext(int numSocket, Path zmqKeyStore) {
        if (instance == null) {
            instance = new SmartContext(numSocket);
        }
        if (zmqKeyStore != null) {
            try {
                instance.checkKeyStore(zmqKeyStore);
            } catch (KeyStoreException | NoSuchAlgorithmException
                            | CertificateException | InvalidKeySpecException
                            | UnrecoverableEntryException | IOException | InvalidKeyException ex) {
                throw new IllegalArgumentException("Can't load the key store", ex);
            }
        }
        return instance;
    }

    private SmartContext(int numSocket) {
        logger.debug("New SmartContext instance");
        context = new ZContext(numSocket);
        localContext = ThreadLocal.withInitial(() -> {
            synchronized(SmartContext.this) {
                ZContext local = ZContext.shadow(context);
                activeContext.incrementAndGet();
                logger.debug("new shadow context rank {}", local, activeContext.get());
                return local;
            }
        });
        // Initiate a top level instance
        firstContext = localContext.get();
        terminator = ThreadBuilder.get()
                        .setDaemon(true)
                        .setName("terminator")
                        .setRunnable(() -> {
                            synchronized (SmartContext.class) {
                                if (instance != null) {
                                    logger.debug("starting shutdown hook for ZMQ");
                                    instance.terminate();
                                }
                            }
                        }).setShutdownHook(true).build();

    }

    private void checkKeyStore(Path zmqKeyStore) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, InvalidKeySpecException, UnrecoverableEntryException, InvalidKeyException {
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
            throw new IllegalArgumentException("Unsupported key store");
        }
        KeyStore ks = KeyStore.getInstance(keystoretype);

        char[] emptypass = new char[] {};
        KeyFactory kf = KeyFactory.getInstance(NaclProvider.NAME);
        PrivateKey prk = null;
        PublicKey puk = null;

        if (! Files.exists(zmqKeyStore)) {
            ks.load(null);

            KeyPairGenerator kpg = KeyPairGenerator.getInstance(kf.getAlgorithm());
            kpg.initialize(256);
            KeyPair kp = kpg.generateKeyPair();
            NaclCertificate certificate = new NaclCertificate(kp.getPublic());

            ks.setKeyEntry("loghubzmqpair", kp.getPrivate(), emptypass, new Certificate[] {certificate});
            try (FileOutputStream ksstream = new FileOutputStream(zmqKeyStore.toFile())) {
                ks.store(ksstream, emptypass);
            }

            prk = kp.getPrivate();
            Path publicKeyPath = Paths.get(zmqKeyStore.toString().replaceAll("\\.[a-z]+$", ".pub"));
            try (PrintWriter writer = new PrintWriter(publicKeyPath.toFile(), "UTF-8")) {
                writer.print(CURVEPREFIX + " ");
                writer.println(Base64.getEncoder().encodeToString(kf.getKeySpec(kp.getPublic(), NaclPublicKeySpec.class).getBytes()));
            }
            puk = kp.getPublic();
        } else {
            try (FileInputStream ksstream = new FileInputStream(zmqKeyStore.toFile())) {
                ks.load(ksstream, emptypass);
            }
            KeyStore.Entry e = ks.getEntry("zmqpair", new KeyStore.PasswordProtection(emptypass));
            if (e instanceof PrivateKeyEntry) {
                prk = ((PrivateKeyEntry) e).getPrivateKey();
                puk = ((PrivateKeyEntry) e).getCertificate().getPublicKey();
            }
        }
        privateKey = kf.getKeySpec(prk, NaclPrivateKeySpec.class).getBytes();
        publicKey = kf.getKeySpec(puk, NaclPublicKeySpec.class).getBytes();
        assert privateKey != null && privateKey.length == Options.CURVE_KEYSIZE;
        assert publicKey != null && publicKey.length == Options.CURVE_KEYSIZE;
    }

    public boolean isRunning() {
        return running;
    }

    @SuppressWarnings("deprecation")
    public Socket newSocket(Method method, Sockets type, String endpoint, int hwm, int timeout) {
        Socket socket = localContext.get().createSocket(type.ordinal());
        String url = endpoint + ":" + type.toString() + ":" + method.getSymbol();
        logger.trace("new socket: {}={}", url, socket);
        socket.setRcvHWM(hwm);
        socket.setSndHWM(hwm);
        socket.setSendTimeOut(timeout);
        socket.setReceiveTimeOut(timeout);
        method.act(socket, endpoint);
        socket.setIdentity(url.getBytes(StandardCharsets.UTF_8));
        return socket;
    }

    public Socket newSocket(Method method, Sockets type, String endpoint) {
        // All socket have high hwm and are blocking
        return newSocket(method, type, endpoint, 1, -1);
    }

    public void setCurveServer(Socket socket) {
        if (privateKey == null || privateKey.length != Options.CURVE_KEYSIZE) {
            throw new IllegalStateException("Curve requested but private key not define");
        }
        if (publicKey == null || publicKey.length != Options.CURVE_KEYSIZE) {
            throw new IllegalStateException("Curve requested but public key not define");
        }
        socket.setCurveServer(true);
        socket.setCurvePublicKey(publicKey);
        socket.setCurveSecretKey(privateKey);
    }

    public void setCurveClient(Socket socket, byte[] serverPublicKey) {
        if (privateKey == null || privateKey.length != Options.CURVE_KEYSIZE) {
            throw new IllegalStateException("Curve mechanism requested but private key not defined");
        }
        if (publicKey == null || publicKey.length != Options.CURVE_KEYSIZE) {
            throw new IllegalStateException("Curve mechanism requested but public key not defined");
        }
        if (serverPublicKey == null || serverPublicKey.length != Options.CURVE_KEYSIZE) {
            throw new IllegalArgumentException("Curve mechanism requested but server public key not defined");
        }
        socket.setCurveServer(false);
        socket.setCurvePublicKey(publicKey);
        socket.setCurveSecretKey(privateKey);
        socket.setCurveServerKey(serverPublicKey);
    }

    public void close(Socket socket) {
        try {
            logger.trace("close socket {}", socket);
            socket.setLinger(0);
            localContext.get().destroySocket(socket);
        } catch (ZMQException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            ZMQHelper.logZMQException(logger, "close " + socket, e);
        } catch (ClosedSelectorException e) {
            logger.debug("in close: {}", () -> e.getMessage());
        } catch (Exception e) {
            logger.error("in close: {}", () -> e.getMessage());
        }
    }

    public void terminate() {
        synchronized (SmartContext.class) {
            try {
                logger.debug("Terminating ZMQ shadow context {}", () -> localContext.get());
                localContext.get().getSockets().forEach(context::destroySocket);
            } catch (ZMQException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                ZMQHelper.logZMQException(logger, "terminate", e);
            } catch (java.nio.channels.ClosedSelectorException e) {
                logger.error("closed selector: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } catch (RuntimeException e) {
                logger.error("Unexpected error: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } finally {
                localContext.get().destroy();
            }
            try {
                if (activeContext.decrementAndGet() == 0) {
                    running = false;
                    logger.debug("Global ZMQ context terminated");
                    Runtime.getRuntime().removeShutdownHook(terminator);
                    System.out.println(context.getSockets());
                    context.destroy();
                }
            } catch (ZMQException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                ZMQHelper.logZMQException(logger, "terminate", e);
            } catch (java.nio.channels.ClosedSelectorException e) {
                logger.error("closed selector: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } catch (RuntimeException e) {
                logger.error("Unexpected error: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.ERROR, e);
            } finally {
                SmartContext.instance = null;
            }
        }
    }

    public ZPoller getZPoller() {
        return new ZPoller(localContext.get());
    }

    public Socket[] getPair(String name) {
        String endPoint = "inproc://pair/" + name;
        Socket socket1 = newSocket(Method.BIND, Sockets.PAIR, endPoint);
        socket1.setLinger(0);
        socket1.setHWM(1);

        Socket socket2 = newSocket(Method.CONNECT, Sockets.PAIR, endPoint);
        socket2.setLinger(0);
        socket2.setHWM(1);

        return new Socket[] {socket1, socket2};
    }

}
