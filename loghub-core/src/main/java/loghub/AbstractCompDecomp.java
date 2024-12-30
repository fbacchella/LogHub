package loghub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;

abstract class AbstractCompDecomp implements Filter {

    @Setter
    @EqualsAndHashCode(callSuper = true) @ToString
    public abstract static class Builder<B extends AbstractCompDecomp> extends AbstractBuilder<B> {
        protected int memoryLimitInKb = -1;
    }

    protected final CompressorStreamFactory csf;

    protected AbstractCompDecomp(Builder<? extends AbstractCompDecomp> builder) {
        csf = new CompressorStreamFactory(true, builder.memoryLimitInKb);
    }

    public byte[] filter(byte[] in, int offset, int length) throws FilterException {
        ByteBuf outb = PooledByteBufAllocator.DEFAULT.compositeBuffer(length);
        try (InputStream ins = source(new ByteArrayInputStream(in, offset, length));
             OutputStream outs = destination(new ByteBufOutputStream(outb))) {
            IOUtils.copy(ins, outs);
        } catch (IOException | CompressorException e) {
            outb.release();
            throw new FilterException("Failed to (de)compress: " + Helpers.resolveThrowableException(e), e);
        }
        try {
            byte[] out = new byte[outb.readableBytes()];
            outb.readBytes(out);
            return out;
        } finally {
            outb.release();
        }
    }

    public ByteBuf filter(ByteBuf in) throws FilterException {
        ByteBuf out = in.alloc().compositeBuffer(in.readableBytes());
        try (InputStream ins = source(new ByteBufInputStream(in));
             OutputStream outs = destination(new ByteBufOutputStream(out))) {
            IOUtils.copy(ins, outs);
        } catch (IOException | CompressorException e) {
            out.release();
            throw new FilterException("Failed to (de)compress: " + Helpers.resolveThrowableException(e), e);
        }
        return out;
    }

    protected abstract InputStream source(InputStream wrappedInput) throws CompressorException;

    protected abstract OutputStream destination(OutputStream wrappedOutput) throws CompressorException;

}
