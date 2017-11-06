package loghub.netflow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.Decoder;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.Tools.ProcessingStatus;

public class ProcessorTest {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void test() throws IOException, DecodeException, ProcessorException, InterruptedException {
        Processor p = new Processor();

        InputStream is = getClass().getResourceAsStream("/netflow/packets/ipfix.dat");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8*1024];
        for (int length; (length = is.read(buffer)) != -1; ){
            out.write(buffer, 0, length);
        }
        ByteBuf bbuffer = Unpooled.wrappedBuffer(out.toByteArray());
        Decoder nfd = new NetflowDecoder();
        IpConnectionContext dummyctx = new IpConnectionContext(new InetSocketAddress(0), new InetSocketAddress(0), null);
        Map<String, Object> content = nfd.decode(dummyctx, bbuffer);

        Event e = Tools.getEvent();
        e.setTimestamp((Date) content.remove(Event.TIMESTAMPKEY));
        e.putAll(content);

        ProcessingStatus ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());
        logger.debug(ps.mainQueue.remove());

        Assert.assertTrue(ps.mainQueue.isEmpty());
    }

}
