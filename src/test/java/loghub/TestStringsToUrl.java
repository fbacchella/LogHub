package loghub;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStringsToUrl {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void testone() {
        URL[] endPoints = Helpers.stringsToUrl(new String[] {"onehost", "otherhost:8080"}, 80, "http", logger);
        Assert.assertEquals("onehost", endPoints[0].getHost());
        Assert.assertEquals("otherhost", endPoints[1].getHost());

        Assert.assertEquals(80, endPoints[0].getPort());
        Assert.assertEquals(8080, endPoints[1].getPort());
        Arrays.toString(endPoints);

        Assert.assertEquals("http://onehost:80", endPoints[0].toString());
        Assert.assertEquals("http://otherhost:8080", endPoints[1].toString());
    }

    @Test
    public void testtwo() {
        URL[] endPoints = Helpers.stringsToUrl(new String[] {"onehost", "http://otherhost:8080"}, -1, "https", logger);
        Assert.assertEquals("onehost", endPoints[0].getHost());
        Assert.assertEquals("otherhost", endPoints[1].getHost());

        Assert.assertEquals(-1, endPoints[0].getPort());
        Assert.assertEquals(8080, endPoints[1].getPort());

        Assert.assertEquals("https://onehost", endPoints[0].toString());
        Assert.assertEquals("http://otherhost:8080", endPoints[1].toString());
    }

}
