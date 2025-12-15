package loghub.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStore.ProtectionParameter;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.zeromq.ZConfig;
import org.zeromq.ZMQ;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.neilalexander.jnacl.crypto.curve25519;
import com.neilalexander.jnacl.crypto.curve25519xsalsa20poly1305;

import fr.loghub.naclprovider.NaclCertificate;
import fr.loghub.naclprovider.NaclPrivateKeySpec;
import fr.loghub.naclprovider.NaclProvider;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import loghub.Helpers;
import loghub.security.ssl.MultiKeyStoreProvider;
import loghub.security.ssl.MultiKeyStoreProvider.SubKeyStore;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQSocketFactory;
import lombok.ToString;

@Parameters
@ToString
public class KeyTool implements BaseParametersRunner {

    private static final char[] password = new char[] {};

    private static final ProtectionParameter protection = new KeyStore.PasswordProtection(password);

    // Ensure that the provider is loaded
    private static final KeyFactory NACLKEYFACTORY = ZMQHelper.NACLKEYFACTORY;

    private static final Map<String, String> MAPPING = Map.of(
            "application/x-java-keystore", "JKS",
            "application/x-java-jce-keystore", "JCEKS",
            "application/x-java-bc-keystore", "BKS",
            "application/x-java-bc-uber-keystore", "Keystore.UBER",
            "application/x-zeromq-zpl", "ZPL"
    );

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--import"}, description = "Import key file")
    private Path keypath = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--name"}, description = "The keyname")
    private String keyname = ZMQSocketFactory.KEYNAME;

    @Parameter(names = {"--generate"}, description = "Generate a new key")
    private boolean generate = false;

    @Parameter(names = {"--export"}, description = "Export key file (can be specified multiple times)")
    private List<Path> exportPaths = List.of();

    @Parameter(names = {"-v", "--verbose"}, description = "Enable verbose output")
    private boolean verbose = false;

    @Override
    public void reset() {
        keypath = null;
        keyname = ZMQSocketFactory.KEYNAME;
        generate = false;
        exportPaths = List.of();
        verbose = false;
    }

    @Override
    public int run(List<String> mainParameters, PrintWriter out, PrintWriter err) {
        if (! mainParameters.isEmpty()) {
            err.format("Unhandled arguments: %s%n", mainParameters.stream().map("'%s'"::formatted).collect(Collectors.joining(" ")));
            return ExitCode.INVALIDARGUMENTS;
        }
        KeyPair k;
        if (generate) {
            try {
                if (verbose) {
                    err.format("Generating new key%n");
                }
                k = generateKey();
            } catch (NoSuchAlgorithmException e) {
                err.format("Unable to generate key %s%n", e.getMessage());
                return ExitCode.CRITICALFAILURE;
            }
        } else if (keypath != null) {
            String mimeType = Helpers.getMimeType(keypath.toString());
            try {
                if (verbose) {
                    err.format("Importing key from \"%s\" (type: %s)%n", keypath, MAPPING.get(mimeType));
                }
                k = switch (mimeType) {
                    case "application/x-zeromq-zpl" -> readFromZpl();
                    case "application/pkcs8" -> readFromP8();
                    case "application/x-java-keystore",
                         "application/x-java-jce-keystore",
                         "application/x-java-bc-keystore",
                         "application/x-java-bc-uber-keystore" -> readFromKeyStore();
                    default -> null;
                };
                if (k == null) {
                    err.format("Unknown key format: %s%n", mimeType);
                    return ExitCode.INVALIDARGUMENTS;
                }
            } catch (GeneralSecurityException e) {
                err.format("Unable to decode key: %s%n", e.getMessage());
                return ExitCode.INVALIDARGUMENTS;
            } catch (IOException e) {
                err.format("Unable to read key file \"%s\": %s%n", keypath, e.getMessage());
                return ExitCode.INVALIDARGUMENTS;
            }
        } else {
            return ExitCode.INVALIDARGUMENTS;
        }
        try {
            if (! exportPaths.isEmpty()) {
                for (Path exportPath : exportPaths) {
                    String mimeType = Helpers.getMimeType(exportPath.toString());
                    if (verbose) {
                        err.format("Exporting to \"%s\" %s (type: %s)%n", exportPath, mimeType, MAPPING.get(mimeType));
                    }
                    switch (mimeType) {
                    case "application/x-zeromq-zpl" -> writeToZpl(exportPath, k);
                    case "application/pkcs8" -> writeToP8(exportPath, k);
                    case "application/x-java-keystore",
                         "application/x-java-jce-keystore",
                         "application/x-java-bc-keystore",
                         "application/x-java-bc-uber-keystore" -> writeToKeystore(exportPath, mimeType, k);
                    default -> {
                        err.format("Unhandled key format: %s%n", mimeType);
                        return ExitCode.INVALIDARGUMENTS;
                    }
                    }
                    if (verbose) {
                        err.format("Exported to \"%s\"%n", exportPath);
                    }
                }
            }
            NaclPublicKeySpec pubspec = NACLKEYFACTORY.getKeySpec(k.getPublic(), NaclPublicKeySpec.class);
            out.println("Curve: " + Base64.getEncoder().encodeToString(pubspec.getBytes()));
            return ExitCode.OK;
        } catch (GeneralSecurityException e) {
            err.format("Unable to decode key: %s%n", e.getMessage());
            return ExitCode.CRITICALFAILURE;
        } catch (IOException e) {
            err.format("Unable to write key file: %s%n", e.getMessage());
            return ExitCode.INVALIDARGUMENTS;
        }
    }

    private void writeToKeystore(Path exportPath, String mimeType, KeyPair k) throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance(MAPPING.get(mimeType));
        ks.load(null);
        NaclCertificate certificate = new NaclCertificate(k.getPublic());
        ks.setKeyEntry(keyname, k.getPrivate(), password, new Certificate[] {certificate});
        ks.store(Files.newOutputStream(exportPath), password);
    }

    private void writeToP8(Path exportPath, KeyPair k) throws IOException {
        Files.write(exportPath, k.getPrivate().getEncoded());
    }

    private void writeToZpl(Path exportPath, KeyPair k) throws GeneralSecurityException, IOException {
        ZConfig zconf = new ZConfig("root", null);
        NaclPublicKeySpec pubSpec = NACLKEYFACTORY.getKeySpec(k.getPublic(), NaclPublicKeySpec.class);
        NaclPrivateKeySpec privSpec = NACLKEYFACTORY.getKeySpec(k.getPrivate(), NaclPrivateKeySpec.class);
        zconf.putValue("curve/public-key", ZMQ.Curve.z85Encode(pubSpec.getBytes()));
        zconf.putValue("curve/secret-key", ZMQ.Curve.z85Encode(privSpec.getBytes()));
        zconf.save(exportPath.toString());
    }

    private KeyPair readFromZpl() throws GeneralSecurityException, IOException {
        ZConfig zpl = ZConfig.load(keypath.toString());
        NaclPrivateKeySpec privSpec = new NaclPrivateKeySpec(ZMQ.Curve.z85Decode(zpl.getValue("curve/secret-key")));
        NaclPublicKeySpec pubSpec = new NaclPublicKeySpec(ZMQ.Curve.z85Decode(zpl.getValue("curve/public-key")));
        PublicKey pubKey = NACLKEYFACTORY.generatePublic(pubSpec);
        PrivateKey privKey = NACLKEYFACTORY.generatePrivate(privSpec);
        return new KeyPair(pubKey, privKey);
    }

    private KeyPair readFromP8() throws GeneralSecurityException, IOException {
        byte[] key = Files.readAllBytes(keypath);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(key);
        PrivateKey privateKey = NACLKEYFACTORY.generatePrivate(keySpec);
        NaclPrivateKeySpec privspec = NACLKEYFACTORY.getKeySpec(privateKey, NaclPrivateKeySpec.class);
        byte[] publicKey = new byte[curve25519xsalsa20poly1305.crypto_secretbox_PUBLICKEYBYTES];
        curve25519.crypto_scalarmult_base(publicKey, privspec.getBytes());
        NaclPublicKeySpec pubspec = new NaclPublicKeySpec(publicKey);
        PublicKey pubKey = NACLKEYFACTORY.generatePublic(pubspec);
        return new KeyPair(pubKey, privateKey);
    }

    private KeyPair readFromKeyStore() throws GeneralSecurityException, IOException {
        SubKeyStore param = new SubKeyStore();
        param.addSubStore(keypath.normalize().toString());
        KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
        ks.load(param);
        PrivateKeyEntry e = (PrivateKeyEntry) ks.getEntry(keyname, protection);
        return new KeyPair(e.getCertificate().getPublicKey(), e.getPrivateKey());
    }

    private KeyPair generateKey() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance(NaclProvider.NAME);
        kpg.initialize(256);
        return kpg.generateKeyPair();
    }

}
