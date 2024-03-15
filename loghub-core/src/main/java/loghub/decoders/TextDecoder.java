package loghub.decoders;

import java.util.Map;
import java.util.stream.Stream;

import loghub.ConnectionContext;

public interface TextDecoder {

    String getCharset();
    Object decodeObject(ConnectionContext<?> ctx,String message) throws DecodeException;
    Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, String message) throws DecodeException;

}
