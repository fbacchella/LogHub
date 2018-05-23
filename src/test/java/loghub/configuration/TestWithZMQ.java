package loghub.configuration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import loghub.Receiver;
import loghub.Sender;
import loghub.Tools;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper.Method;
import zmq.socket.Sockets;

public class TestWithZMQ {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ");
    }

    @Test(timeout=3000) 
    public void testSimpleInput() throws InterruptedException, ConfigException, IOException, ExecutionException {
        Properties conf = Tools.loadConf("simpleinput.conf");
        logger.debug("pipelines: {}", conf.pipelines);

        for(Receiver r: conf.receivers) {
            Assert.assertTrue("failed to configure " + r, r.configure(conf));
            r.start();
        }
        for(Sender s: conf.senders) {
            Assert.assertTrue("failed to configure " + s, s.configure(conf));
            s.start();
        }
        try (Socket out = tctxt.ctx.newSocket(Method.CONNECT, Sockets.SUB, "inproc://sender", 1, -1);
             Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Sockets.PUB, "inproc://listener", 1, -1);
        ) {
            out.subscribe(new byte[]{});
            // Wait for ZMQ to be started
            Thread.sleep(30);
            sender.send("something");
            Event received = conf.mainQueue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull("nothing received", received);
            conf.outputQueues.get("main").add(received);
            byte[] buffer = out.recv();
            Assert.assertEquals("wrong send message", "something", new String(buffer));
        }
        for(Receiver r: conf.receivers) {
            r.stopReceiving();
        }
        for(Sender s: conf.senders) {
            s.stopSending();
        }
        Assert.assertTrue(SmartContext.getContext().terminate().get());
    }

}
