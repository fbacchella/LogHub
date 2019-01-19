package loghub.processors;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestConvert {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.SyslogPriority");
    }
    
    private void check(String className, Class<?> reference, String invalue, Object outvalue) throws ProcessorException {
        Convert cv = new Convert();
        cv.setField(new String[] {"message"});
        cv.setClassName(className);

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(cv.configure(props));

        Event e = Tools.getEvent();
        e.put("message",invalue);
        e.process(cv);
        Assert.assertTrue(reference.isAssignableFrom(e.get("message").getClass()));
        Assert.assertTrue(e.get("message").getClass().isAssignableFrom(reference));
        Assert.assertEquals(outvalue, e.get("message"));
    }

    @Test
    public void TestResolution() throws ProcessorException, UnknownHostException {
        check("java.lang.Integer", Integer.class, "38", Integer.valueOf(38));
        check("java.lang.Byte", Byte.class, "38", Byte.valueOf((byte) 38));
        check("java.lang.Short", Short.class, "38", Short.valueOf((short) 38));
        check("java.lang.Long", Long.class, "38", Long.valueOf((long) 38));
        check("java.lang.Float", Float.class, "38", Float.valueOf((float) 38));
    }

    @Test(expected=loghub.ProcessorException.class)
    public void TestInvalid() throws ProcessorException, UnknownHostException {
        check("java.net.InetAddress", java.net.InetAddress.class, "127.0.0.1", InetAddress.getByName("127.0.0.1"));
    }

    @Test(expected=loghub.ProcessorException.class)
    public void TestInvalidNumber() throws ProcessorException, UnknownHostException {
        check("java.lang.Integer", java.lang.Integer.class, "a", "");
    }

}
