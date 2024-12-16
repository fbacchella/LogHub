package loghub.processors;

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(XPathExtractor.Builder.class)
public class XPathExtractor extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<XPathExtractor> {
        String xpath;
        @Override
        public XPathExtractor build() {
            return new XPathExtractor(this);
        }
    }
    public static XPathExtractor.Builder getBuilder() {
        return new XPathExtractor.Builder();
    }

    private final ThreadLocal<XPathExpression> localExpression;

    private XPathExtractor(Builder builder) {
        // Validate the expression
        XPathFactory xPathFactory = XPathFactory.newInstance();
        getXPathExpression(xPathFactory, builder.xpath);
        localExpression = ThreadLocal.withInitial(() -> getXPathExpression(xPathFactory, builder.xpath));
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            localExpression.get();
        } catch (IllegalArgumentException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log(Helpers.resolveThrowableException(ex));
            return false;
        } finally {
            localExpression.remove();
        }
        return super.configure(properties);
    }

    private XPathExpression getXPathExpression(XPathFactory xPathFactory, String expression) {
        XPath xPath = xPathFactory.newXPath();
        try {
            return xPath.compile(expression);
        } catch (XPathExpressionException ex) {
            throw new IllegalArgumentException("Not a valid xpath: " + expression, ex);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (! (value instanceof Node)) {
            throw IgnoredEventException.INSTANCE;
        } else {
            try {
                NodeList nodes = (NodeList) localExpression.get().evaluate(value, XPathConstants.NODESET);
                if (nodes.getLength() == 1) {
                    return resolveNode(nodes.item(0));
                } else {
                    List<Object> nodesValues = new ArrayList<>(nodes.getLength());
                    for (int i = 0 ; i < nodes.getLength(); i++) {
                        nodesValues.add(resolveNode(nodes.item(i)));
                    }
                    return nodesValues;
                }
            } catch (XPathExpressionException e) {
                throw event.buildException("Failed to parse expression", e);
            }
        }
    }

    private Object resolveNode(Node n) {
        switch (n.getNodeType()) {
        case Node.TEXT_NODE:
        case Node.COMMENT_NODE:
            return n.getNodeValue();
        default:
            return n;
        }
    }
}
