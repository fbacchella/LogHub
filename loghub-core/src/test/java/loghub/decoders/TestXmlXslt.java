package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.metrics.Stats;

public class TestXmlXslt {

    private static Logger logger;

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Stats.reset();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "com.fasterxml", "loghub");
    }

    @Test
    public void readPom() throws IOException, DecodeException {
        XmlXslt.Builder builder = XmlXslt.getBuilder();
        XmlXslt dec = builder.build();
        dec.configure(new Properties(Collections.emptyMap()), null);

        Map<String, Object> ev = dec.decode(ConnectionContext.EMPTY, Files.readAllBytes(Paths.get("pom.xml")))
                              .findAny().get();
        Assert.assertTrue(ev.containsKey("project"));
    }

    @Test
    public void readBroken() {
        XmlXslt.Builder builder = XmlXslt.getBuilder();
        XmlXslt dec = builder.build();
        dec.configure(new Properties(Collections.emptyMap()), null);

        DecodeException ex = Assert.assertThrows(DecodeException.class,
                () -> dec.decode(ConnectionContext.EMPTY, "<xml>\n<bla".getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals("Failed to read xml document", ex.getMessage());
        Assert.assertEquals("XML document structures must start and end within the same entity.", ex.getCause().getMessage());
    }

    @Test
    public void test_loghub_decoders_XmlDom() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.XmlXslt"
                , BeanChecks.BeanInfo.build("xslt", String.class)
        );
    }

}
