package loghub.receivers;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;

public class TestKafka {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.Kafka");
    }

    @Ignore
    @Test
    public void testone() throws InterruptedException, IOException {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        Kafka r = new Kafka(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        r.setDecoder(new StringCodec());
        r.setBrokers(new String[] {"192.168.0.13"});
        r.setTopic("test");
        Assert.assertTrue("Failed to configure trap receiver", r.configure(new Properties(Collections.emptyMap())));
        r.start();
        while (true) {
            Event e = receiver.take();
            System.out.println(e);
        }
        //r.close();
    }

}
