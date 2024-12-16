package loghub;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import lombok.Setter;
import lombok.experimental.Accessors;

public class XmlHandler implements ErrorHandler {

    @Accessors(fluent = false, chain = true)
    @Setter
    public static class Builder {
        boolean nameSpaceAware = true;
        private Logger logger;
        public XmlHandler build() {
            return new XmlHandler(this);
        }
    }
    public static XmlHandler.Builder getBuilder() {
        return new XmlHandler.Builder();
    }

    private final Logger logger;
    private final ThreadLocal<DocumentBuilder> localDocumentBuilder;

    protected XmlHandler(Builder builder) {
        this.logger = builder.logger;
        // Dom parsing must be done outside XSLT transformation, error handling is a mess and unusable
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Focus on content, not structure
        factory.setIgnoringComments(true);
        factory.setValidating(false);
        factory.setIgnoringElementContentWhitespace(true);
        factory.setCoalescing(true);
        factory.setExpandEntityReferences(false);
        factory.setNamespaceAware(builder.nameSpaceAware);
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (ParserConfigurationException ex) {
            throw new IllegalStateException("Incomplete XML implementation", ex);
        }
        localDocumentBuilder = ThreadLocal.withInitial(() -> getDocumentBuilder(factory));
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

    private void handlingSaxException(Level level, SAXParseException exception) {
        logger.atLevel(level)
                .withThrowable(logger.isDebugEnabled() ? exception : null)
                .log("Document parsing failed: line: {}; column: {}: {}",
                        exception::getLineNumber, exception::getColumnNumber, exception::getMessage
                );
    }

    public Document parse(InputSource is) throws IOException, SAXException {
        return localDocumentBuilder.get().parse(is);
    }

    public Document parse(InputStream is) throws IOException, SAXException {
        return localDocumentBuilder.get().parse(is);
    }

    public void remove() {
        localDocumentBuilder.remove();
    }

    @Override
    public void warning(SAXParseException exception) {
        handlingSaxException(Level.INFO, exception);
    }

    @Override
    public void error(SAXParseException exception) {
        handlingSaxException(Level.WARN, exception);
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXParseException {
        throw exception;
    }
}
