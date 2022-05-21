package loghub.security.ssl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PKCS8Codec extends SimpleBerCodec {

    // In older JVM, some mapping where missing, needs to explicit them
    private static final Map<String, String> algoMapping = new HashMap<>();
    static {
        algoMapping.put("1.2.840.113549.1.1.1","RSA");
        algoMapping.put("1.3.101.112","Ed25519");
        algoMapping.put("1.2.840.10045.2.1","EC");
    }

    private int[] oid;
    private byte[] key;

    public PKCS8Codec(ByteBuffer buffer) {
        super(buffer);
    }

    public void write() {
        writeSequence(buffer, i -> {
            writeInteger(i, 0);
            writeSequence(i, j -> {
                writeOid(j, oid);
            });
            writeOctetString(i, key);
        });
    }

    public void read() {
        readSequence(buffer, i -> {
            int version = readInteger(i);
            if (version != 0) {
                throw new IllegalStateException("Only PKCS#8 version 0 supported");
            }
            readSequence(i, j-> {
                oid = readOid(j);
            });
            key = readOctetString(i);
        });
    }

    /**
     * A cheating resolver, only resolve possible element from KeyFactory algorithm known to JVM 17 and BouncyCastle
     * @param element
     * @return
     */
    @Override
    protected int resolveOidPart(byte[] element) {
        StringBuilder buffer = new StringBuilder(element.length *2);
        for (byte b: element) {
            buffer.append(String.format("%02x", b));
        }
        String elemStr = buffer.toString();
        switch (elemStr) {
        case "8104":
            return 132;
        case "8105":
            return 133;
        case "8648":
            return 840;
        case "c06d":
            return 8301;
        case "ce38":
            return 10040;
        case "ce3d":
            return 10045;
        case "ce3e":
            return 10046;
        case "818e33":
            return 18227;
        case "81b01a":
            return 22554;
        case "86f70d":
            return 113549;
        default:
            throw new IllegalArgumentException(elemStr);
        }
    }

    public int[] getAlgoOid() {
        return oid.clone();
    }

    public String getAlgo() {
        String algoOid = Arrays.stream(oid).mapToObj(Integer::toString).collect(Collectors.joining("."));
        return algoMapping.getOrDefault(algoOid, algoOid);
    }

    public void setAlgoOid(int[] oid) {
        this.oid = oid.clone();
    }

    public byte[] getPrivateKey() {
        return key.clone();
    }

    public void setPrivateKey(byte[] key) {
        this.key = key.clone();
    }

}
