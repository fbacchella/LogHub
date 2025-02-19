package loghub.zmq;

import java.lang.reflect.InvocationTargetException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.ProviderException;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

import org.zeromq.ZMQ;

import fr.loghub.naclprovider.NaclCertificate;
import fr.loghub.naclprovider.NaclProvider;
import fr.loghub.naclprovider.NaclPublicKeySpec;
import zmq.socket.Sockets;

public class ZMQHelper {

    public static final String CURVEPREFIX = "Curve";

    public static final KeyFactory NACLKEYFACTORY;
    static {
        try {
            Security.insertProviderAt((Provider) Class.forName("fr.loghub.naclprovider.NaclProvider").getConstructor().newInstance(), Security.getProviders().length + 1);
            NACLKEYFACTORY = KeyFactory.getInstance(NaclProvider.NAME);
        } catch (NoSuchAlgorithmException | InstantiationException | IllegalAccessException | ClassNotFoundException |
                 NoSuchMethodException | InvocationTargetException e) {
            throw new ProviderException("NaclProvider unavailable", e);
        }
    }

    public static class SocketInfo {
        public final Method method;
        public final Sockets type;
        public final String endpoint;
        public SocketInfo(Method method, Sockets type, String endpoint) {
            super();
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
    }

    public enum Method {
        CONNECT {
            @Override
            public boolean act(ZMQ.Socket socket, String address) {
                return socket.connect(address);
            }

            @Override
            public char getSymbol() {
                return '-';
            }
        },
        BIND {
            @Override
            public boolean act(ZMQ.Socket socket, String address) {
                return socket.bind(address);
            }

            @Override
            public char getSymbol() {
                return 'O';
            }
        };
        public abstract boolean act(ZMQ.Socket socket, String address);
        public abstract char getSymbol();
    }

    private ZMQHelper() {
    }

    public static Certificate parseServerIdentity(String information) {
        String[] keyInfos = information.split(" +");
        if (CURVEPREFIX.equals(keyInfos[0]) && keyInfos.length == 2) {
            try {
                byte[] serverPublicKey = Base64.getDecoder().decode(keyInfos[1].trim());
                NaclPublicKeySpec keyspec = new NaclPublicKeySpec(serverPublicKey);
                return new NaclCertificate(NACLKEYFACTORY.generatePublic(keyspec));
            } catch (IllegalArgumentException | InvalidKeyException | InvalidKeySpecException e) {
                throw new IllegalArgumentException("Not a valid curve server key: " + e.getMessage(), e);
            }
        } else {
            throw new IllegalArgumentException("Not a valid server key");
        }
    }

    public static byte[] getPublicKey(Certificate certificate) {
        try {
            NaclPublicKeySpec specs = NACLKEYFACTORY.getKeySpec(certificate.getPublicKey(), NaclPublicKeySpec.class);
            return specs.getBytes();
        } catch (InvalidKeySpecException e) {
            throw new IllegalArgumentException("Not a valid public key");
        }
    }

    public static String makeServerIdentity(Certificate cert) throws InvalidKeySpecException {
        StringBuilder builder = new StringBuilder();
        NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(cert.getPublicKey(), NaclPublicKeySpec.class);

        builder.append(CURVEPREFIX + " ");
        builder.append(Base64.getEncoder().encodeToString(pubkey.getBytes()));
        return builder.toString();
    }

    public static String makeServerIdentityZ85(Certificate cert) throws InvalidKeySpecException {
        NaclPublicKeySpec pubkey = NACLKEYFACTORY.getKeySpec(cert.getPublicKey(), NaclPublicKeySpec.class);
        return ZMQ.Curve.z85Encode(pubkey.getBytes());
    }

}
