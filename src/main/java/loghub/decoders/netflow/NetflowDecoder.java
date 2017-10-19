package loghub.decoders.netflow;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import loghub.Decoder;

public class NetflowDecoder extends Decoder {

    @Override
    public Map<String, Object> decode(ConnectionContext ctx, byte[] msg, int offset, int length) throws DecodeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Object> decode(ConnectionContext ctx, ByteBuf bbuf) throws DecodeException {
        // TODO Auto-generated method stub
        return null;
    }

}
