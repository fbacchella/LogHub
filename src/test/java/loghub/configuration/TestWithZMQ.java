package loghub.configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.ContextRule;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.ZMQFlow;
import loghub.ZMQSink;
import loghub.receivers.Receiver;
import loghub.senders.Sender;
import loghub.zmq.ZMQHelper.Method;

public class TestWithZMQ {

    private static Logger logger;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.receivers.ZMQ", "loghub.senders.ZMQ", "loghub.ZMQSink", "loghub.ZMQFlow");
    }

    private CountDownLatch latch;

    public boolean process(Socket socket, int eventMask) {
        while ((socket.getEvents() & ZPoller.IN) != 0) {
            String received = socket.recvStr();
            logger.trace("received {}", received);
            latch.countDown();
            if (received == null) {
                Error error = Error.findByCode(socket.errno());
                logger.error("error with ZSocket {}: {}", socket, error.getMessage());
                return false;
            }
        }
        return true;
    }

    @Ignore
    @Test(timeout=3000) 
    public void testSimpleInput() throws InterruptedException, ConfigException, IOException, ExecutionException {
        latch = new CountDownLatch(1);

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

        AtomicInteger count = new AtomicInteger(0);
        ZMQFlow.Builder flowbuilder = new ZMQFlow.Builder()
                        .setMethod(Method.CONNECT)
                        .setDestination("inproc://listener")
                        .setType(SocketType.PUB)
                        .setSource(() -> String.format("message %s", count.incrementAndGet()).getBytes(StandardCharsets.UTF_8))
                        .setMsPause(250)
                        .setCtx(tctxt.ctx);

        ZMQSink.Builder sinkbuilder = new ZMQSink.Builder()
                        .setMethod(Method.CONNECT)
                        .setType(SocketType.SUB)
                        .setTopic(new byte[] {})
                        .setSource("inproc://sender")
                        .setLocalhandler(this::process)
                        .setCtx(tctxt.ctx);

        try (ZMQSink receiver = sinkbuilder.build();
             ZMQFlow sender = flowbuilder.build();
            ) {
            Event received = conf.mainQueue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull("nothing received", received);
            conf.outputQueues.get("main").add(received);
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            //byte[] buffer = out.recv();
            //Assert.assertEquals("wrong send message", "something", new String(buffer));
            for(Receiver r: conf.receivers) {
                r.stopReceiving();
            }
            for(Receiver r: conf.receivers) {
                r.close();
            }
            for(Sender s: conf.senders) {
                s.stopSending();
            }
        }
    }

}
