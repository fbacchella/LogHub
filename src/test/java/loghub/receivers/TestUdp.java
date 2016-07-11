package loghub.receivers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
import loghub.decoders.StringCodec;

public class TestUdp {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Udp", "loghub.Receiver");
    }

    @Test(timeout=500)
    public void testone() throws InterruptedException, IOException {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(10);
        Udp r = new Udp(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        r.setListen(InetAddress.getLocalHost().getHostAddress());
        r.setDecoder(new StringCodec());
        r.start();
        try(DatagramSocket send = new DatagramSocket()) {
            byte[] buf = "message".getBytes();
            InetAddress address = InetAddress.getLocalHost();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, address, r.getPort());
            try {
                int port;
                int count = 0;
                while((port = r.getPort()) == 0 && count++ < 10) {
                    Thread.sleep(10);
                }
                packet.setPort(port);
                send.send(packet);
            } catch (IOException e1) {
                logger.error("IO exception on port {}", r.getPort());
                throw e1;
            }
        }
        Event e = receiver.take();
        r.interrupt();
        Assert.assertEquals("Missing message", "message", e.get("message"));
        Assert.assertTrue(e.containsKey("host"));
    }
}
