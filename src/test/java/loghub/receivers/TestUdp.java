package loghub.receivers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.NamedArrayBlockingQueue;
import loghub.Tools;
import loghub.decoders.StringCodec;
import loghub.receivers.Udp;

public class TestUdp {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.Udp", "loghub.Receiver");
    }

    @Test(timeout=500)
    public void testone() throws InterruptedException, IOException {
        NamedArrayBlockingQueue receiver = new NamedArrayBlockingQueue("out.listener1");
        Udp r = new Udp(receiver);
        r.setListen(InetAddress.getLocalHost().getHostAddress());
        r.setDecoder(new StringCodec());
        r.start();
        try(DatagramSocket send = new DatagramSocket()) {
            byte[] buf = "message".getBytes();
            InetAddress address = InetAddress.getLocalHost();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, address, r.getPort());
            packet.setPort(r.getPort());
            send.send(packet);
        }
        Event e = receiver.take();
        r.interrupt();
        Assert.assertEquals("Missing message", "message", e.get("message"));
    }
}
