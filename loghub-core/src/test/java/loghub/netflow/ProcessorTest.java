package loghub.netflow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.Tools.ProcessingStatus;
import loghub.decoders.DecodeException;
import loghub.decoders.Decoder;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class ProcessorTest {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test(timeout=1000)
    public void test() throws IOException, DecodeException {
        Processor p = new Processor();

        ByteBuf bbuffer;
        try (InputStream is = getClass().getResourceAsStream("/netflow/packets/ipfix.dat");
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8*1024];
            for (int length; (length = is.read(buffer)) != -1; ){
                out.write(buffer, 0, length);
            }
            bbuffer = Unpooled.wrappedBuffer(out.toByteArray());
        }
        Decoder nfd = NetflowDecoder.getBuilder().build();
        IpConnectionContext dummyctx = new IpConnectionContext(new InetSocketAddress(0), new InetSocketAddress(0), null);
        nfd.decode(dummyctx, bbuffer).forEach(content -> {
            Event e = factory.newEvent();
            e.setTimestamp((Date) content.remove(Event.TIMESTAMPKEY));
            e.putAll(content);
            ProcessingStatus ps;
            try {
                ps = Tools.runProcessing(e, "main", Collections.singletonList(p));
            } catch (ProcessorException ex) {
                throw new RuntimeException(ex);
            }
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());
            logger.debug(ps.mainQueue.remove());

            Assert.assertTrue(ps.mainQueue.isEmpty());
        });
    }

}
