package loghub;

import java.nio.charset.StandardCharsets;

import org.apache.commons.compress.MemoryLimitException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestAbstractCompDecomp {

    @Test
    public void testRountTripBytes() throws FilterException {
        byte[] input = "Compressed message".getBytes(StandardCharsets.UTF_8);
        Decompressor.Builder builder = Decompressor.getBuilder();
        Decompressor dcomp = builder.build();

        Compressor.Builder cbuilder = Compressor.getBuilder();
        cbuilder.setFormat(CompressorStreamFactory.DEFLATE);
        Compressor comp = cbuilder.build();

        byte[] result = dcomp.filter(comp.filter(input));
        Assert.assertArrayEquals(input, result);
    }

    @Test
    public void testRountTripByteBuf() throws FilterException {
        byte[] input = "Compressed message".getBytes(StandardCharsets.UTF_8);
        ByteBuf inbuf = Unpooled.buffer(input.length);
        inbuf.writeBytes(input);

        Decompressor.Builder builder = Decompressor.getBuilder();
        Decompressor dcomp = builder.build();

        Compressor.Builder cbuilder = Compressor.getBuilder();
        cbuilder.setFormat(CompressorStreamFactory.DEFLATE);
        Compressor comp = cbuilder.build();

        ByteBuf resultbuff = dcomp.filter(comp.filter(inbuf));
        try {
            byte[] result = new byte[resultbuff.readableBytes()];
            resultbuff.readBytes(result);
            Assert.assertArrayEquals(input, result);
        } finally {
            resultbuff.release();
        }
    }

    @Test(expected = MemoryLimitException.class)
    public void testTooBig() throws Throwable {
        byte[] input = "Compressed message".getBytes(StandardCharsets.UTF_8);
        Compressor.Builder cbuilder = Compressor.getBuilder();
        cbuilder.setFormat(CompressorStreamFactory.XZ);
        Compressor comp = cbuilder.build();

        Decompressor.Builder builder = Decompressor.getBuilder();
        builder.setMemoryLimitInKb(1);
        Decompressor dcomp = builder.build();

        byte[] result;
        try {
            result = dcomp.filter(comp.filter(input));
        } catch (FilterException e) {
            throw e.getCause();
        }
        Assert.assertArrayEquals(input, result);
    }

}
