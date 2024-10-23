package loghub.netty.transport;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.transport.POLLER;

public class TestPoller {

    private static final boolean IS_MAC;
    private static final boolean IS_LINUX;

    static {
        String operatingSystem = System.getProperty("os.name", "");
        IS_MAC = operatingSystem.startsWith("Mac");
        IS_LINUX = operatingSystem.startsWith("Linux");
     }

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
    }

    @Test
    public void testPollers() {
        Assert.assertTrue(POLLER.NIO.isAvailable());
        Assert.assertTrue(POLLER.OIO.isAvailable());
        Assert.assertEquals(IS_MAC, POLLER.KQUEUE.isAvailable());
        Assert.assertEquals(IS_LINUX, POLLER.EPOLL.isAvailable());
    }

}
