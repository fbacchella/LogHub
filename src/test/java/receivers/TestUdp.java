package receivers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.ContextRule;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.decoders.StringCodec;
import loghub.receivers.Udp;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestUdp {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.Udp", "loghub.Receiver");
    }

    @Test(timeout=500)
    public void testone() throws InterruptedException, IOException {
        Socket receiver = tctxt.ctx.newSocket(Method.BIND, Type.PULL, "inproc://out.listener1");
        Map<byte[], Event> eventQueue = new HashMap<>();
        Udp r = new Udp();
        r.setEndpoint("inproc://out.listener1");
        r.setListen(InetAddress.getLocalHost().getHostAddress());
        r.start(eventQueue);
        r.setDecoder(new StringCodec());
        Thread.sleep(30);
        try(DatagramSocket send = new DatagramSocket()) {
            byte[] buf = "message 1".getBytes();
            InetAddress address = InetAddress.getLocalHost();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, 
                                            address, r.getPort());
            packet.setPort(r.getPort());
            send.send(packet);
        }
        byte[] key = receiver.recv();
        Event e = eventQueue.get(key);
        Assert.assertEquals("Missing message", "message 1", e.get("message"));
    }
}
