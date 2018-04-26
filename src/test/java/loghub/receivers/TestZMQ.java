package loghub.receivers;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

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
import loghub.Pipeline;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.decoders.StringCodec;
import loghub.zmq.ZMQHelper.Method;
import zmq.socket.Sockets;

public class TestZMQ {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.ContextRule");
    }

    private void dotest(Consumer<ZMQ> configure, Socket sender) throws InterruptedException  {
        BlockingQueue<Event> receiver = new ArrayBlockingQueue<>(1);
        ZMQ r = new ZMQ(receiver, new Pipeline(Collections.emptyList(), "testone", null));
        configure.accept(r);
        r.setDecoder(new StringCodec());
        Assert.assertTrue(r.configure(new Properties(Collections.emptyMap())));
        r.start();
        Thread.sleep(30);
        Assert.assertTrue(sender.send("message 1"));
        Event e = receiver.take();
        Assert.assertEquals("Missing message", "message 1", e.get("message"));
        r.stopReceiving();
    }

    @Test(timeout=500)
    public void testone() throws InterruptedException {
        Socket sender = tctxt.ctx.newSocket(Method.BIND, Sockets.PUB, "inproc://listener1");
        dotest(r -> {
            r.setListen("inproc://listener1");
            r.setMethod("CONNECT");
            r.setType("SUB");
        }, sender);
    }

    @Test(timeout=500)
    public void testtwo() throws InterruptedException {
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Sockets.PUB, "inproc://listener1");
        dotest(r -> {
            r.setListen("inproc://listener1");
        }, sender);
    }

}
