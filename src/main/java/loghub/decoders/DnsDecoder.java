package loghub.decoders;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.xbill.DNS.Message;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.configuration.Beans;

@Beans({"field"})
public class DnsDecoder extends Decoder {

    private String field = "message";

    @Override
    public Map<String, Object> decode(ConnectionContext connectionContext, byte[] msg, int offset, int length) throws DecodeException {
        try {
            Message m = new Message(msg);
            return Collections.singletonMap(getField(), m);
        } catch (IOException e) {
            throw new DecodeException("IO exception while reading DNS packet content: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> decode(ConnectionContext connectionContext, ByteBuf bbuf) throws DecodeException {
        if(bbuf.isDirect()) {
            int length = bbuf.readableBytes();
            byte[] buffer = new byte[length];
            bbuf.getBytes(bbuf.readerIndex(), buffer);
            return decode(connectionContext, buffer);
        } else {
            return decode(connectionContext, bbuf.array());
        }
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
