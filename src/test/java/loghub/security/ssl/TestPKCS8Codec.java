package loghub.security.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.stream.Stream;

import org.junit.Test;

public class TestPKCS8Codec {

    @Test
    public void testRead() throws IOException, NoSuchAlgorithmException {
        Stream<String> algos = Stream.of(
                "DSA",
                "DiffieHellman",
                "EC",
                "Ed25519",
                "Ed448",
                "EdDSA",
                "RSA",
                "RSASSA-PSS",
                "X25519",
                "X448",
                "XDH"
        );
        algos.forEach(a -> {
            try {
                boolean available;
                // check that the algo is available in this JVM
                try {
                    KeyFactory.getInstance(a);
                    available = true;
                } catch (NoSuchAlgorithmException e) {
                    available = false;
                }
                if (available) {
                    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(a);
                    KeyPair kp = keyGen.generateKeyPair();
                    byte[] content = kp.getPrivate().getEncoded();
                    PKCS8Codec codec = new PKCS8Codec(ByteBuffer.wrap(content));
                    codec.read();
                }
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalArgumentException(ex);
            }
        });
    }

    // for s in *.pem; do
    // d=${s%.pem}.pk8
    // openssl pkcs8 -in $s -inform pem -out $d -outform der -nocrypt -topk8
    // done
    @Test
    public void fromFile() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        String[] files = {
                "pkcs8/DSA_2048.pk8",
                "pkcs8/DSA_4096.pk8",
                "pkcs8/ED25519.pk8",
                "pkcs8/RSA_2048.pk8",
                "pkcs8/RSA_4096.pk8",
                "pkcs8/prime192v1.pk8",
                "pkcs8/prime256v1.pk8",
                "pkcs8/secp224r1.pk8",
                "pkcs8/secp384r1.pk8",
                "pkcs8/secp521r1.pk8"
        };
        for (String f: files) {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(f)) {
                try {
                    byte[] buffer = new byte[4096];
                    is.read(buffer);
                    PKCS8Codec codec = new PKCS8Codec(ByteBuffer.wrap(buffer));
                    codec.read();
                    PKCS8EncodedKeySpec keyspec = new PKCS8EncodedKeySpec(buffer);
                    KeyFactory kf = KeyFactory.getInstance(codec.getAlgo());
                    kf.generatePrivate(keyspec);
                } catch (NoSuchAlgorithmException e) {
                    // No ED25519 on java 1.9
                    if ("pkcs8/ED25519.pk8".equals(f) && System.getProperty("java.version").startsWith("1.8.")) {
                        continue;
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

}
