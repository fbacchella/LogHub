package loghub.decoders;

import java.util.Map;
import java.util.stream.Stream;

import loghub.ConnectionContext;

public interface TextDecoder {

    public String getCharset();
    public Object decodeObject(ConnectionContext<?> ctx,String message) throws DecodeException;
    public Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, String message) throws DecodeException;

}
