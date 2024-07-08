package loghub.jackson;

import java.io.IOException;
import java.net.InetAddress;

import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;

public class MsgPackIpDeserializer implements ExtensionTypeCustomDeserializers.Deser  {
    @Override
    public Object deserialize(byte[] data) throws IOException {
        return InetAddress.getByAddress(data);
    }
}
