package loghub.security.ssl;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class SimpleBerCodec {

    protected final ByteBuffer buffer;

    protected static final byte INTEGER = 0x02;
    protected static final byte BITSTRING = 0x03;
    protected static final byte OCTETSTRING = 0x04;
    protected static final byte OBJECTIDENTIFIER = 0x06;
    protected static final byte SEQUENCE = 0x30;

    protected SimpleBerCodec(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public abstract void write();

    public abstract void read();

    protected int[] readOid(ByteBuffer topbuffer) {
        return readType(OBJECTIDENTIFIER, topbuffer,  i-> {
            int elementcount = 2;
            int[] oid = new int[i.remaining() + 1];
            byte first = i.get();
            oid[1] = first % 40;
            oid[0] = (first - oid[1]) / 40;
            for (int j = 1; i.remaining() > 0 && j < oid.length - 1; j++) {
                byte next = i.get();
                if (next >= 0) {
                    oid[j + 1] = next;
                } else {
                    // back on step
                    i.position(i.position() - 1);
                    i.mark();
                    int count = 0;
                    while (i.get() < 0) {
                        count++;
                    }
                    i.reset();
                    byte[] buffer = new byte[count + 1];
                    i.get(buffer);
                    // Resolve OID in a specific way.
                    // It allows optimisation, OID encoding is overcomplicated
                    oid[j + 1] = resolveOidPart(buffer);
                }
                elementcount++;
            }
            return Arrays.copyOf(oid, elementcount);
        });
    }

    protected abstract int resolveOidPart(byte[] element);

    protected void readSequence(ByteBuffer topbuffer, Consumer<ByteBuffer> consum) {
        readType(SEQUENCE, topbuffer,  i-> {consum.accept(i); return null;});
    }

    protected byte[] readBitString(ByteBuffer topbuffer) {
        Function<ByteBuffer, byte[]> scan = i -> {
            byte[] val = new byte[i.remaining()];
            i.get(val);
            return val;
        };
        return readType(BITSTRING, topbuffer, scan);
    }

    protected byte[] readOctetString(ByteBuffer topbuffer) {
        Function<ByteBuffer, byte[]> scan = i -> {
            byte[] val = new byte[i.remaining()];
            i.get(val);
            return val;
        };
        return readType(OCTETSTRING, topbuffer, scan);
    }

    protected Integer readInteger(ByteBuffer topbuffer) {
        Function<ByteBuffer, Integer> scan = i -> {
            Integer subval = (int) i.get();
            return subval;
        };
        return readType(INTEGER, topbuffer, scan);
    }

    private <T> T readType(byte type, ByteBuffer topbuffer, Function<ByteBuffer, T> consum) {
        byte foundtype = topbuffer.get();
        if (foundtype != type) {
            throw new IllegalStateException("Expected " + type + ", got " +  foundtype);
        }
        int sequenceLength = topbuffer.get();
        if (sequenceLength == 0 || sequenceLength == 0xff) {
            throw new IllegalArgumentException("Invalid ASN1 object, invalid content definition");
        }
        if (sequenceLength > topbuffer.remaining()) {
            throw new IllegalArgumentException("Invalid ASN1 object, oversized content");
        }
        if (sequenceLength < 0) {
            int count = sequenceLength & 0x7F;
            // The max possible value without the first byte should be less that remaining
            // It's a strict inferior value to real size
            if (1L<<(8*(count-1)) > topbuffer.remaining()) {
                throw new IllegalArgumentException("Invalid ASN1 object, oversized content");
            }
            byte[] buffer = new byte[count];
            topbuffer.get(buffer);
            BigInteger bigSequenceLength = new BigInteger(1, buffer);
            sequenceLength = bigSequenceLength.intValue();
        }
        ByteBuffer subbuffer = topbuffer.slice();
        topbuffer.position(topbuffer.position() + sequenceLength);
        subbuffer.limit(sequenceLength);
        return consum.apply(subbuffer);
    }

    protected void writeOid(ByteBuffer topbuffer, int[] oid) {
        topbuffer.put((byte) OBJECTIDENTIFIER);
        topbuffer.put((byte) (oid.length - 1));
        topbuffer.put((byte) (oid[0] * 40 + oid[1]));
        for (int i= 2 ; i < oid.length; i++) {
            topbuffer.put((byte) oid[i]);
        }
    }

    protected void writeSequence(ByteBuffer topbuffer, Consumer<ByteBuffer> fill) {
        writeType(SEQUENCE, topbuffer, fill);
    }

    protected void writeBitString(ByteBuffer topbuffer, byte[] data) {
        writeType(BITSTRING, topbuffer, i -> {
            i.put(data);
        });
    }

    protected void writeOctetString(ByteBuffer topbuffer, byte[] data) {
        writeType(OCTETSTRING, topbuffer, i -> {
            i.put(data);
        });
    }

    protected void writeInteger(ByteBuffer topbuffer, long value) {
        writeType(INTEGER, topbuffer, i -> {
            i.put((byte) value);
        });
    }

    private void writeType(byte type, ByteBuffer topbuffer, Consumer<ByteBuffer> fill) {
        topbuffer.put(type);
        int sizepos = topbuffer.position();
        topbuffer.put((byte) 0); // place holder for size;
        int startpos = topbuffer.position();
        fill.accept(topbuffer);
        int endpos = topbuffer.position();
        topbuffer.put(sizepos, (byte) (endpos - startpos));
    }

}
