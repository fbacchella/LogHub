package loghub;

import io.netty.buffer.ByteBuf;
import loghub.decoders.DecodeException;

public interface Filter {
    byte[] filter(byte[] input, int offset, int length) throws DecodeException;
    default byte[] filter(byte[] input) throws DecodeException {
        return filter(input, 0, input.length);
    }
    ByteBuf filter(ByteBuf input) throws DecodeException;
}
