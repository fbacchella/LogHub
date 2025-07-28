package loghub.cbor;

import java.io.IOException;
import java.math.BigInteger;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public abstract class AbstractBigIntegerTagHandler extends CborTagHandler<BigInteger> {

    AbstractBigIntegerTagHandler(int tag) {
        super(tag);
    }

    @Override
    public void write(BigInteger data, CBORGenerator p) throws IOException {
        byte[] buffer = data.abs().toByteArray();
        p.writeBytes(buffer, 0, buffer.length);
    }

    @Override
    public BigInteger parse(CBORParser p) throws IOException {
        byte[] data = p.getBinaryValue();
        BigInteger value = new BigInteger(data);
        return getTag() == 2 ? value : value.negate();
    }

}
