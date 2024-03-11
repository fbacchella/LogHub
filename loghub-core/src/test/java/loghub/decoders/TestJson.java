package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

public class TestJson {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "com.fasterxml", "loghub");
    }

    @Test
    public void testArray() throws DecodeException {
        Json.Builder builder = Json.getBuilder();
        builder.setCharset("UTF-8");
        Json dec = builder.build();

        String message = "[{\"a\":1}, {\"a\": 2}]";
        List<Object> objects = dec.decode(ConnectionContext.EMPTY, message.getBytes(StandardCharsets.UTF_8))
                          .collect(Collectors.toList());
        Assert.assertEquals(2, objects.size());
        Map<Object, Object> o1 = (Map<Object, Object>) objects.get(0);
        Assert.assertEquals(1, o1.get("a"));
        Map<Object, Object> o2 = (Map<Object, Object>) objects.get(1);
        Assert.assertEquals(2, o2.get("a"));
    }

    @Test
    public void test_loghub_decoders_Json() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Json"
                , BeanChecks.BeanInfo.build("charset", String.class)
                , BeanChecks.BeanInfo.build("field", String.class)
        );
    }

}
