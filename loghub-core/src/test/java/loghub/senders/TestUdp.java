package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.encoders.ToJson;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

public class TestUdp {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.Udp");
    }

    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

    private DatagramChannel ssocket;
    private int port;
    private Selector selector;

    @Before
    public void connect() throws IOException {
        ssocket = DatagramChannel.open();
        ssocket.bind(new InetSocketAddress("localhost", 0));
        port = ssocket.socket().getLocalPort();
        selector = Selector.open();
    }

    @After
    public void close() throws IOException {
        ssocket.close();
        selector.close();
    }

    @Test
    public void readdata() throws IOException, InterruptedException {
        Properties props = new Properties(Collections.emptyMap());
        ToJson.Builder encoderBuilder = ToJson.getBuilder();
        encoderBuilder.setPretty(false);
        ToJson encoder = encoderBuilder.build();

        Udp.Builder builder = Udp.getBuilder();
        builder.setDestination("localhost");
        builder.setPort(port);
        builder.setEncoder(encoder);

        Udp sender = builder.build();
        sender.setInQueue(queue);

        Assert.assertTrue(sender.configure(props));
        Stats.registerSender(sender);
        sender.start();

        Event ev = factory.newEvent();
        ev.put("message", 1);
        queue.add(ev);
        ev = factory.newEvent();
        ev.put("message", 2);
        queue.add(ev);

        ByteBuffer content = ByteBuffer.allocate(100);
        ssocket.configureBlocking(false);
        ssocket.register(selector, SelectionKey.OP_READ);
        while (true) {
            if (selector.select(100) > 0) {
                while (ssocket.receive(content) != null) {
                    Thread.sleep(10);
                }
            } else {
                break;
            }
        }
        ssocket.close();
        content.flip();
        byte[] dst = new byte[content.remaining()];
        content.get(dst);
        String message = new String(dst, StandardCharsets.UTF_8);
        Assert.assertEquals("{\"message\":1}{\"message\":2}", message);
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.Tcp"
                , BeanChecks.BeanInfo.build("destination", String.class)
                , BeanChecks.BeanInfo.build("port", Integer.TYPE)
        );
    }

}
