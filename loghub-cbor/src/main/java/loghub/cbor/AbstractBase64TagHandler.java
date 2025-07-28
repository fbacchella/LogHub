package loghub.cbor;

import java.io.IOException;
import java.util.Base64;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public abstract class AbstractBase64TagHandler extends CborTagHandler<byte[]> {

    AbstractBase64TagHandler(int tag) {
        super(tag);
    }

    @Override
    public byte[] parse(CBORParser p) throws IOException {
        Base64.Decoder decoder = getTag() == 33 ? Base64.getUrlDecoder() : Base64.getDecoder();
        return decoder.decode(p.getText());
    }

    @Override
    public void write(byte[] data, CBORGenerator p) throws IOException {
        Base64.Encoder encoder = getTag() == 33 ? Base64.getUrlEncoder() : Base64.getEncoder();
        p.writeString(encoder.encodeToString(data));
    }

}
