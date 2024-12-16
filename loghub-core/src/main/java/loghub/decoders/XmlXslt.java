package loghub.decoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.logging.log4j.Level;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.jackson.JacksonBuilder;
import loghub.receivers.Receiver;
import lombok.Setter;

@BuilderClass(XmlXslt.Builder.class)
public class XmlXslt extends AbstractXmlDecoder<XmlXslt, XmlXslt.Builder> implements ErrorListener {

    @Setter
    public static class Builder extends AbstractXmlDecoder.Builder<XmlXslt> {
        private String xslt;
        @Override
        public XmlXslt build() {
            return new XmlXslt(this);
        }
    }

    public static XmlXslt.Builder getBuilder() {
        return new XmlXslt.Builder();
    }

    private final ThreadLocal<Transformer> localTransformer;
    private final ObjectReader mapper;
    private Charset xsltcharset;

    protected XmlXslt(XmlXslt.Builder builder) {
        super(builder);
        TransformerFactory tFactory = TransformerFactory.newInstance();
        tFactory.setErrorListener(this);
        localTransformer = ThreadLocal.withInitial(() -> getTransformer(builder.xslt, tFactory));
        mapper = JacksonBuilder.get(JsonMapper.class).getReader();
    }

    @Override
    public boolean configure(Properties properties, Receiver<?, ?> receiver) {
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
        } finally {
            localTransformer.remove();
            handler.remove();
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
            Document d = handler.parse(is);
            DOMSource source = new DOMSource(d);
            Transformer transformer = tFactory.newTransformer(source);
            transformer.setErrorListener(this);
            return transformer;
        } catch (TransformerConfigurationException | IOException | SAXException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    protected Object domTransform(Document d) throws DecodeException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DOMSource source = new DOMSource(d);
            localTransformer.get().transform(source, new StreamResult(baos));
            String output = baos.toString(xsltcharset);
            return mapper.readValue(output);
        } catch (TransformerException | IOException ex) {
            throw new DecodeException("Failed to read xml document", ex);
        }
    }

    // Custom handling of transformer exception, avoiding unreadable stacks.
    private void handlingTransformerException(Level level, TransformerException exception) {
        logger.atLevel(level)
              .withThrowable(logger.isDebugEnabled() ? exception : null)
              .log(exception.getMessage());
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
    public void fatalError(TransformerException exception) throws TransformerException {
        throw  exception;
    }

}
