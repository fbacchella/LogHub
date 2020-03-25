package loghub.decoders;

import java.util.Map;
import java.util.stream.Stream;

import loghub.ConnectionContext;
import loghub.decoders.Decoder.DecodeException;

public interface TextDecoder {

    public String getCharset();
    public Object decodeObject(ConnectionContext<?> ctx,String message) throws DecodeException;
    public Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, String message) throws DecodeException;

}
