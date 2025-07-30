package loghub.cbor;

import java.io.IOException;
import java.math.BigInteger;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

public abstract class AbstractBigIntegerTagHandler extends CborTagHandler<BigInteger> {

    AbstractBigIntegerTagHandler(int tag) {
        super(tag);
    }

    @Override
    public void write(BigInteger data, CBORGenerator p) throws IOException {
        byte[] buffer = data.abs().toByteArray();
        p.writeBinary(buffer);
    }

    @Override
    public BigInteger parse(CborParser p) throws IOException {
        byte[] data = p.readBytes();
        BigInteger value = new BigInteger(data);
        return getTag() == 2 ? value : value.negate();
    }

}
