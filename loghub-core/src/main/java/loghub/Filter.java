package loghub;

import io.netty.buffer.ByteBuf;

public interface Filter {
    byte[] filter(byte[] input, int offset, int length) throws FilterException;
    default byte[] filter(byte[] input) throws FilterException {
        return filter(input, 0, input.length);
    }
    ByteBuf filter(ByteBuf input) throws FilterException;
}
