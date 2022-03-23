package loghub.security.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
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

}
