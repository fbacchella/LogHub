package loghub;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class Decompressor extends AbstractCompDecomp {

    public static class Builder extends AbstractCompDecomp.Builder<Decompressor> {
         public Decompressor build() {
            return new Decompressor(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public Decompressor(Builder builder) {
        super(builder);
    }

    protected InputStream source(InputStream wrappedInput) throws CompressorException {
        return csf.createCompressorInputStream(CompressorStreamFactory.detect(wrappedInput), wrappedInput);
    }

    protected OutputStream destination(OutputStream wrappedOutput) {
        return wrappedOutput;
    }

}
