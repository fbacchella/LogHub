package loghub;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.zeromq.ZConfig;

import fr.loghub.naclprovider.NaclCertificate;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQSocketFactory;
import lombok.Getter;

import static loghub.zmq.ZMQSocketFactory.KEYNAME;

public class ZMQFactory extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    private ZMQSocketFactory factory = null;
    @Getter
    private Path securityFolder;
    @Getter
    private Path rootFolder;
    private final String subFolder;
    private final TemporaryFolder testFolder;

    public ZMQFactory(TemporaryFolder testFolder, String subFolder) {
        assert subFolder != null && testFolder != null;
        this.testFolder = testFolder;
        this.subFolder = subFolder;
    }

    public ZMQFactory() {
        this.testFolder = null;
        this.subFolder = null;
    }

    @Override
    protected void before() throws Throwable {
        if (testFolder != null) {
            rootFolder = Paths.get(testFolder.getRoot().getAbsolutePath());
            securityFolder = rootFolder.resolve(subFolder);
            Files.createDirectories(securityFolder);
            Files.createDirectories(securityFolder.resolve("certs"));
        }
    }

    @Override
    protected void after() {
        logger.debug("Terminating ZMQ manager");
        if (factory != null) {
            factory.close();
        }
        logger.debug("Test finished");
    }

    public ZMQSocketFactory getFactory() {
        if (factory == null) {
            if (securityFolder != null) {
                factory = ZMQSocketFactory
                                  .builder()
                                  .zmqKeyStore(securityFolder.resolve("zmqtest.jks"))
                                  .withZap(true)
                                  .zmqCertsDir(securityFolder.resolve("certs"))
                                  .build();
            } else {
                factory = new ZMQSocketFactory();
            }
        }
        return factory;
    }

    public PrivateKeyEntry createKeyStore(Path zmqKeyStore, Map<String, String> properties) throws
            GeneralSecurityException, IOException {
        zmqKeyStore = rootFolder.resolve(zmqKeyStore);
        KeyStore ks = KeyStore.getInstance("JKS");

        char[] emptypass = new char[] {};

        logger.debug("Creating a new keystore at {}", zmqKeyStore);
        ks.load(null);

        KeyPairGenerator kpg = KeyPairGenerator.getInstance(ZMQHelper.NACLKEYFACTORY.getAlgorithm());
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
        ZConfig zpl = new ZConfig("root", null);
        properties.forEach(zpl::putValue);
        zpl.putValue("/curve/public-key", ZMQHelper.makeServerIdentityZ85(certificate));
        zpl.save(publicKeyZplPath.toString());
        PrivateKey prk = kp.getPrivate();
        return new PrivateKeyEntry(prk, new NaclCertificate[] {certificate});
    }

    public String loadServerPublicKey() {
        getFactory();
        Path keyPubpath = securityFolder.resolve("zmqtest.pub");
        try (ByteArrayOutputStream pubkeyBuffer = new ByteArrayOutputStream()) {
            Files.copy(keyPubpath, pubkeyBuffer);
            return pubkeyBuffer.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
