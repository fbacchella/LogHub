package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
import loghub.ConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.ToJson;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestTcp {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.Tcp");
    }

    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

    private ServerSocketChannel ssocket;
    private int port;
    private Selector selector;

    @Before
    public void connect() throws IOException {
        ssocket = ServerSocketChannel.open();
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
        ToJson.Builder encoderBuilder = ToJson.getBuilder();
        encoderBuilder.setPretty(false);
        ToJson encoder = encoderBuilder.build();

        Tcp.Builder builder = Tcp.getBuilder();
        builder.setDestination("localhost");
        builder.setPort(port);
        builder.setEncoder(encoder);
        builder.setSeparator("\n\r");

        Tcp sender = builder.build();
        sender.setInQueue(queue);

        Assert.assertTrue(sender.configure(new Properties(Collections.emptyMap())));
        sender.start();

        Event ev = factory.newEvent(ConnectionContext.EMPTY);
        ev.put("message", 1);
        queue.add(ev);
        ev = factory.newEvent(ConnectionContext.EMPTY);
        ev.put("message", 2);
        queue.add(ev);

        ByteBuffer content = ByteBuffer.allocate(100);
        SocketChannel channel = ssocket.accept();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
        while (true) {
            if (selector.select(100) > 0) {
                while (channel.read(content) > 0) {
                    Thread.sleep(10);
                }
            } else {
                break;
            }
        }
        channel.close();
        content.flip();
        byte[] dst = new byte[content.remaining()];
        content.get(dst);
        String message = new String(dst, StandardCharsets.UTF_8);
        Assert.assertEquals("{\"message\":1}\n\r{\"message\":2}\n\r", message);
    }

    @Test(timeout=2000)
    public void testEncodeError() throws IOException, InterruptedException, EncodeException {
        Tcp.Builder builder = Tcp.getBuilder();
        builder.setDestination("localhost");
        builder.setPort(port);
        builder.setSeparator("\n\r");
        SenderTools.send(builder);
    }

    @Test
    public void test_loghub_senders_Tcp() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.Tcp"
                , BeanChecks.BeanInfo.build("destination", String.class)
                , BeanChecks.BeanInfo.build("port", Integer.TYPE)
                , BeanChecks.BeanInfo.build("separator", String.class)
        );
    }

}
