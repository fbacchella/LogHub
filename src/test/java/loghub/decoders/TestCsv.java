package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
import loghub.decoders.Csv.Builder;

public class TestCsv {

    private static Logger logger;


    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testNoHeader() throws DecodeException {
        Builder builder = Csv.getBuilder();
        builder.setColumns(new String[] {"a", "b", "c"});
        builder.setCharset("UTF-8");
        builder.setHeader(false);
        Csv dec = builder.build();
        String values = "1,2,3\n4,5,6\n";
        Stream<Map<String, Object>> so = dec.decode(ConnectionContext.EMPTY, values.getBytes(StandardCharsets.UTF_8));
        @SuppressWarnings("unchecked")
        Map<String, Object>[] read = so.toArray(i -> new HashMap[i]);
        Assert.assertEquals("1", read[0].get("a"));
        Assert.assertEquals("2", read[0].get("b"));
        Assert.assertEquals("3", read[0].get("c"));
        Assert.assertEquals("4", read[1].get("a"));
        Assert.assertEquals("5", read[1].get("b"));
        Assert.assertEquals("6", read[1].get("c"));
    }

    @Test
    public void testWithHeader() throws DecodeException {
        Builder builder = Csv.getBuilder();
        builder.setCharset("UTF-8");
        builder.setSeparator(';');
        builder.setHeader(true);
        Csv dec = builder.build();
        String values = "a;b;c\n1;2;3\n4;5;6\n";
        Stream<Map<String, Object>> so = dec.decode(ConnectionContext.EMPTY, values.getBytes(StandardCharsets.UTF_8));
        @SuppressWarnings("unchecked")
        Map<String, Object>[] read = so.toArray(i -> new HashMap[i]);
        Assert.assertEquals("1", read[0].get("a"));
        Assert.assertEquals("2", read[0].get("b"));
        Assert.assertEquals("3", read[0].get("c"));
        Assert.assertEquals("4", read[1].get("a"));
        Assert.assertEquals("5", read[1].get("b"));
        Assert.assertEquals("6", read[1].get("c"));
    }

    @Test
    public void test_loghub_decoders_Csv() throws ClassNotFoundException, IntrospectionException, InvocationTargetException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Csv"
                , BeanChecks.BeanInfo.build("columns", BeanChecks.LSTRING)
                , BeanChecks.BeanInfo.build("features", BeanChecks.LSTRING)
                , BeanInfo.build("separator", Character.TYPE)
                , BeanInfo.build("lineSeparator", String.class)
                , BeanInfo.build("nullValue", String.class)
                , BeanInfo.build("header", Boolean.TYPE)
                , BeanInfo.build("charset", String.class)
                );
    }

}
