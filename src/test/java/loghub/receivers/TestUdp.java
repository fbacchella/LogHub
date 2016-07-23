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

    @Test(timeout=1000)
    public void testone() throws InterruptedException, IOException {
        
        // Generate a locally binded random socket
        DatagramSocket socket = new DatagramSocket(0, InetAddress.getByName("192.168.0.14"));
        String hostname = socket.getLocalAddress().getHostAddress();
        int port = socket.getLocalPort();
        socket.close();
        InetSocketAddress destaddr = new InetSocketAddress(hostname, port);
        
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(10);
        Udp r = new Udp(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        r.setHost(hostname);
        r.setPort(port);
        r.setDecoder(new StringCodec());
        Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
        r.start();
        try(DatagramSocket send = new DatagramSocket()) {
            byte[] buf = "message".getBytes();
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
        r.interrupt();
        Assert.assertEquals("Missing message", "message", e.get("message"));
        Assert.assertTrue("didn't find valid hosts informations", e.get("host") instanceof InetAddress);
    }
}
