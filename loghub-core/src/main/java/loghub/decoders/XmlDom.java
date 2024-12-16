package loghub.decoders;

import org.w3c.dom.Document;

import loghub.BuilderClass;

@BuilderClass(XmlDom.Builder.class)
public class XmlDom extends AbstractXmlDecoder<XmlDom, XmlDom.Builder> {

    public static class Builder extends AbstractXmlDecoder.Builder<XmlDom> {
        @Override
        public XmlDom build() {
            return new XmlDom(this);
        }
    }

    public static XmlDom.Builder getBuilder() {
        return new XmlDom.Builder();
    }

    protected XmlDom(XmlDom.Builder builder) {
        super(builder);
    }

    @Override
    protected Object domTransform(Document d) {
        return d;
    }

}
