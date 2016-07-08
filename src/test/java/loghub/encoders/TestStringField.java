package loghub.encoders;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;

public class TestStringField {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testone() {
        StringField encoder = new StringField();
        encoder.setCharset("UTF-16");
        encoder.setFormat("${K1}: ${K2%02d}");
        Event e = Tools.getEvent();
        e.put("K1", "V1");
        e.put("K2", 2);

        byte[] result = encoder.encode(e);

        String formatted = new String(result, Charset.forName("UTF-16"));
        Assert.assertEquals("Formatting failed", "V1: 02", formatted);
    }

}
