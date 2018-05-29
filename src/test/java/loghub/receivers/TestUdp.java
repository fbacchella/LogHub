package loghub.receivers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;

public class TestUdp {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Udp", "loghub.Receiver", "loghub.netty");
    }

    Udp receiver;

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    private void testsend(int size) throws IOException, InterruptedException {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(10);
        Udp r = new Udp(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        // Generate a locally binded random socket
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
            String hostname = socket.getLocalAddress().getHostAddress();
            int port = Tools.tryGetPort();
            InetSocketAddress destaddr = new InetSocketAddress(hostname, port);

            r.setBufferSize(size + 10);
            r.setHost(hostname);
            r.setPort(port);
            r.setDecoder(new StringCodec());
            Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
            r.start();
            int originalMessageSize = 0;
            try(DatagramSocket send = new DatagramSocket()) {
                StringBuilder buffer = new StringBuilder();
                while (buffer.length() <= size) {
                    buffer.append("message");
                }
                byte[] buf = buffer.toString().getBytes();
                originalMessageSize = buffer.length();
                DatagramPacket packet = new DatagramPacket(buf, buf.length, destaddr);
                try {
                    logger.debug("Listening on {}", r.getListenAddress());
                    send.send(packet);
                    logger.debug("One message sent to {}", packet.getAddress());
                } catch (IOException e1) {
                    logger.error("IO exception on port {}", r.getPort());
                    throw e1;
                }
            }
            Event e = receiver.take();
            Assert.assertTrue(Tools.isRecent.apply(e.getTimestamp()));
            Assert.assertTrue("Invalid message content", e.get("message").toString().startsWith("message"));
            Assert.assertEquals("Invalid message size", originalMessageSize, e.get("message").toString().length());
            Assert.assertTrue("didn't find valid hosts informations", e.get("host") instanceof InetAddress);
        } finally {
            r.close();
        }
    }

    @Test(timeout=1000)
    public void testsmall() throws InterruptedException, IOException {
        testsend(1500);
    }

    @Test(timeout=1000)
    public void testbig() throws InterruptedException, IOException {
        testsend(16384);
    }

}
