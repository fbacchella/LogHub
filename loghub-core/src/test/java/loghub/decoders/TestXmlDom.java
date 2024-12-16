package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;

public class TestXmlDom {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test
    public void readPom() throws IOException, DecodeException {
        XmlDom.Builder builder = XmlDom.getBuilder();
        builder.setField("domtree");
        XmlDom dec = builder.build();
        var objects = dec.decode(ConnectionContext.EMPTY, Files.readAllBytes(Paths.get("pom.xml")))
                                       .collect(Collectors.toList());
        var ev = objects.get(0);
        Document d = (Document) ev.get("domtree");
        Assert.assertEquals("project", d.getDocumentElement().getTagName());
    }

    @Test
    public void test_loghub_decoders_XmlDom() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.XmlDom"
                , BeanChecks.BeanInfo.build("nameSpaceAware", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("field", String.class)
        );
    }

}
