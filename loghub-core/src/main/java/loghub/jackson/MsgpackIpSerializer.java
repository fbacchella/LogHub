package loghub.jackson;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackGenerator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MsgpackIpSerializer extends JsonSerializer<InetAddress>  {
    public static final byte EXTENSION_TYPE = 1;
    /**
     * Method that can be called to ask implementation to serialize
     * values of type this serializer handles.
     *
     * @param value       Value to serialize; can <b>not</b> be null.
     * @param gen         Generator used to output resulting Json content
     * @param serializers Provider that can be used to get serializers for
     *                    serializing Objects value contains, if any.
     */
    @Override
    public void serialize(InetAddress value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        byte[] addressBytes = value.getAddress();
        MessagePackExtensionType ext = new MessagePackExtensionType(EXTENSION_TYPE, Arrays.copyOf(addressBytes, addressBytes.length));
        ((MessagePackGenerator)gen).writeExtensionType(ext);
    }
}
