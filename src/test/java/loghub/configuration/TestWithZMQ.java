package loghub.configuration;

import java.io.IOException;

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
import loghub.zmq.ZMQHelper.Type;

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

    @Test(timeout=2000) 
    public void testSimpleInput() throws InterruptedException {
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
        Socket out = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender", 1, -1);
        out.subscribe(new byte[]{});
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener", 1, -1);
        Thread.sleep(30);
        sender.send("something");
        Thread.sleep(30);
        Event received = conf.mainQueue.remove();
        conf.outputQueues.get("main").add(received);
        byte[] buffer = out.recv();
        Assert.assertEquals("wrong send message", "something", new String(buffer));
        tctxt.ctx.close(sender);
        tctxt.ctx.close(out);
        for(Receiver r: conf.receivers) {
            r.interrupt();
        }
        for(Sender s: conf.senders) {
            s.interrupt();
        }
        SmartContext.getContext().terminate();
    }

}
