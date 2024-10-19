package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.logging.log4j.Level;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.jackson.JacksonBuilder;
import loghub.receivers.Receiver;
import lombok.Setter;

@BuilderClass(XmlXslt.Builder.class)
public class XmlXslt extends Decoder implements ErrorListener, ErrorHandler {

    private interface GetInputStream {
        InputStream get() throws IOException;
    }

    private static final String CATCHED = "__CATCHED__";
    private static class CatchedException extends RuntimeException {
        CatchedException(Exception parent) {
            super(CATCHED, parent);
        }
    }

    @Setter
    public static class Builder extends Decoder.Builder<XmlXslt> {
        private String xslt;
        @Override
        public XmlXslt build() {
            return new XmlXslt(this);
        }
    }

    public static XmlXslt.Builder getBuilder() {
        return new XmlXslt.Builder();
    }

    private final ThreadLocal<DocumentBuilder> localDocumentBuilder;
    private final ThreadLocal<Transformer> localTransformer;
    private final ObjectReader mapper;
    private Charset xsltcharset;

    protected XmlXslt(XmlXslt.Builder builder) {
        super(builder);
        // Dom parsing must be done outside of XSLT transformation, error handling is a mess and unusable
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Focus on content, not structure
        factory.setIgnoringComments(true);
        factory.setValidating(false);
        factory.setIgnoringElementContentWhitespace(true);
        factory.setCoalescing(true);
        factory.setExpandEntityReferences(false);
        factory.setNamespaceAware(true);
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (ParserConfigurationException ex) {
            throw new IllegalStateException("Incomplete XML implementation", ex);
        }
        localDocumentBuilder = ThreadLocal.withInitial(() -> getDocumentBuilder(factory));

        TransformerFactory tFactory = TransformerFactory.newInstance();
        tFactory.setErrorListener(this);
        localTransformer = ThreadLocal.withInitial(() -> getTransformer(builder.xslt, tFactory));
        mapper = JacksonBuilder.get(JsonMapper.class).getReader();
    }

    @Override
    public boolean configure(Properties properties, Receiver receiver) {
        try {
            Transformer transformer = localTransformer.get();
            String method = transformer.getOutputProperty("method");
            if (!"text".equals(method)) {
                logger.error("Unhandled encoding method for the xslt: {}", method);
                return false;
            }
            String encoding = transformer.getOutputProperty("encoding");
            xsltcharset  = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;
            return super.configure(properties, receiver);
        } catch (IllegalArgumentException ex) {
            logger.warn("Unusable xslt: {}", ex::getMessage);
            logger.catching(Level.DEBUG, ex);
            return false;
        }
    }

    private InputStream getXsltStream(String xslt) throws IOException {
        if (xslt != null) {
            return Helpers.fileUri(xslt).toURL().openStream();
        } else {
            // using a default transformer found at https://www.bjelic.net/2012/08/01/coding/convert-xml-to-json-using-xslt/
            return XmlXslt.class.getResourceAsStream("/xml2json.xsl");
        }
    }

    private Transformer getTransformer(String xslt, TransformerFactory tFactory) {
        try (InputStream is = getXsltStream(xslt)) {
            Document d = localDocumentBuilder.get().parse(is);
            DOMSource source = new DOMSource(d);
            Transformer transformer = tFactory.newTransformer(source);
            transformer.setErrorListener(this);
            return transformer;
        } catch (TransformerConfigurationException | IOException | SAXException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private DocumentBuilder getDocumentBuilder(DocumentBuilderFactory factory) {
        try {
            DocumentBuilder documentBuilder = factory.newDocumentBuilder();
            documentBuilder.setErrorHandler(this);
            return documentBuilder;
        } catch (ParserConfigurationException ex) {
            throw new IllegalStateException(ex);
        }
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
        try (InputStream is = getis.get();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ){
            Document d = localDocumentBuilder.get().parse(is);
            DOMSource source = new DOMSource(d);
            localTransformer.get().transform(source, new StreamResult(baos));
            String output = baos.toString(xsltcharset);
            return mapper.readValue(output);
        } catch (CatchedException ex) {
            // Exception already logged
            throw new DecodeException("Failed to read xml document", ex.getCause());
        } catch (TransformerException | IOException | SAXException ex) {
            // Exception already logged
            throw new DecodeException("Failed to read xml document", ex);
        }
    }

    // Custom handling of transformer exception, avoiding unreadable stacks.
    private void handlingTransformerException(Level level, TransformerException exception) {
        if (exception.getCause() instanceof CatchedException) {
            throw (CatchedException) exception.getCause();
        } else {
            logger.log(level, exception.getMessage());
            logger.catching(level == Level.FATAL ? Level.FATAL: Level.DEBUG, exception);
            throw new CatchedException(exception);
        }
    }

    @Override
    public void warning(TransformerException exception) {
        handlingTransformerException(Level.DEBUG, exception);
    }

    @Override
    public void error(TransformerException exception) {
        handlingTransformerException(Level.WARN, exception);
    }

    @Override
    public void fatalError(TransformerException exception) {
        handlingTransformerException(Level.ERROR, exception);
    }

    private void handlingSaxException(Level level, SAXParseException exception) {
        if (exception.getCause() instanceof CatchedException) {
            throw (CatchedException) exception.getCause();
        } else {
            logger.log(level, "Document parsing failed: line: {}; column: {}: {}",
                    exception::getLineNumber, exception::getColumnNumber, exception::getMessage);
            logger.catching(level == Level.FATAL ? Level.FATAL: Level.DEBUG, exception);
            throw new CatchedException(exception);
        }
    }

    @Override
    public void warning(SAXParseException exception) {
        handlingSaxException(Level.DEBUG, exception);
    }

    @Override
    public void error(SAXParseException exception) {
        handlingSaxException(Level.WARN, exception);
    }

    @Override
    public void fatalError(SAXParseException exception) {
        handlingSaxException(Level.ERROR, exception);
    }

}
