package loghub.processors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;

import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.XmlHandler;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(ParseXml.Builder.class)
public class ParseXml extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<ParseXml> {
        boolean nameSpaceAware = true;
        public ParseXml build() {
            return new ParseXml(this);
        }
    }
    public static ParseXml.Builder getBuilder() {
        return new ParseXml.Builder();
    }

    XmlHandler handler;

    private ParseXml(Builder builder) {
        super(builder);
        handler = XmlHandler.getBuilder()
                            .setLogger(logger)
                            .setNameSpaceAware(builder.nameSpaceAware)
                            .build();
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            InputSource is;
            if (value instanceof byte[]) {
                is = new InputSource(new ByteArrayInputStream((byte[]) value));
            } else if (value instanceof URI || value instanceof URL) {
                is = new InputSource(value.toString());
            } else if (value instanceof Node) {
                return value;
            } else {
                is = new InputSource(new StringReader(value.toString()));
            }
            return handler.parse(is);
        } catch (SAXParseException ex) {
            // The .toString() of SAXParseException must be used to get useful details
            throw event.buildException("Failed to parse XML: " + ex, ex);
        } catch (IOException | SAXException ex) {
            throw event.buildException("Failed to parse XML: " + ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return "ParseXml";
    }

}
