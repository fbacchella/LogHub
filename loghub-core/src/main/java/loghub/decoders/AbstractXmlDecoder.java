package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;
import loghub.XmlHandler;
import lombok.Setter;

public abstract class AbstractXmlDecoder<X extends AbstractXmlDecoder<X, XB>, XB extends AbstractXmlDecoder.Builder<X>> extends Decoder {

    private interface GetInputStream {
        InputStream get();
    }

    @Setter
    public abstract static class Builder<X extends Decoder> extends Decoder.Builder<X> {
        boolean nameSpaceAware = true;
    }

    XmlHandler handler;

    protected AbstractXmlDecoder(XB builder) {
        super(builder);
        handler = XmlHandler.getBuilder()
                            .setLogger(logger)
                            .setNameSpaceAware(builder.nameSpaceAware)
                            .build();
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return parse(() -> new ByteBufInputStream(bbuf));
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return parse(() -> new ByteArrayInputStream(msg, offset, length));
    }

    private Object parse(GetInputStream getis) throws DecodeException {
        try (InputStream is = getis.get()){
            Document d = handler.parse(is);
            return domTransform(d);
        } catch (IOException | SAXException ex) {
            throw new DecodeException("Failed to read xml document", ex);
        }
    }

    protected abstract Object domTransform(Document d) throws DecodeException;

}
