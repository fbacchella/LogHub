package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.decoders.Properties.Builder;

public class TestProperties {

    private static Logger logger;


    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testNoDefaultArray() throws DecodeException {
        Builder builder = Properties.getBuilder();
        builder.setKeyValueSeparator(":");
        builder.setPathSeparator("->");
        builder.setFirstArrayOffset(2);
        builder.setCharset("UTF-8");
        Properties dec = builder.build();
        String values = "a->2->b: first\na->3->c: second";
        Stream<Map<String, Object>> so = dec.decode(ConnectionContext.EMPTY, values.getBytes(StandardCharsets.UTF_8));
        @SuppressWarnings("unchecked")
        Map<String, List<Map<String, Object>>>[] read = so.toArray(i -> new HashMap[i]);
        Assert.assertEquals(1, read.length);
        Map<String, List<Map<String, Object>>> props = read[0];
        Assert.assertEquals(1, props.keySet().size());
        List<Map<String, Object>> v = props.get("a");
        Assert.assertEquals(2, v.size());
        Map<String, Object> b = v.get(0);
        Map<String, Object> c = v.get(1);
        Assert.assertEquals("first", b.get("b"));
        Assert.assertEquals("second", c.get("c"));
    }

    @Test
    public void test_loghub_decoders_Properties() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Properties"
                , BeanInfo.build("keyValueSeparator", String.class)
                , BeanInfo.build("pathSeparator", String.class)
                , BeanInfo.build("parseSimpleIndexes", Boolean.TYPE)
                , BeanInfo.build("firstArrayOffset", Integer.TYPE)
                , BeanInfo.build("charset", String.class)
                );
    }

}
