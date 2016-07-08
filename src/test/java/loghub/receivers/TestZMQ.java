package loghub.receivers;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.ContextRule;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.decoders.StringCodec;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestZMQ {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.ZMQ", "loghub.Receiver");
    }

    @Ignore
    @Test(timeout=500)
    public void testone() throws InterruptedException {
        Socket sender = tctxt.ctx.newSocket(Method.BIND, Type.PUB, "inproc://listener1");
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        ZMQ r = new ZMQ(receiver, null);
        r.setListen("inproc://listener1");
        r.setDecoder(new StringCodec());
        r.setMethod("CONNECT");
        r.setType("SUB");
        r.start();
        Thread.sleep(30);
        sender.send("message 1");
        Event e = receiver.take();
        Assert.assertEquals("Missing message", "message 1", e.get("message"));
        tctxt.ctx.close(sender);
    }

    @Ignore
    @Test(timeout=500)
    public void testtwo() throws InterruptedException {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        ZMQ r = new ZMQ(receiver, null);
        r.setListen("inproc://listener1");
        r.setDecoder(new StringCodec());
        r.start();
        Thread.sleep(30);
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener1");
        Thread.sleep(30);
        sender.send("message 1");
        Event e = receiver.take();
        Assert.assertEquals("Missing message", "message 1", e.get("message"));
        tctxt.ctx.close(sender);
    }

}
