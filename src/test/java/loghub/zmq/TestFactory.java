package loghub.zmq;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.zmq.ZMQHelper.Method;

public class TestFactory {


    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq");
    }

    @Test(timeout=5000)
    public void testPushPull() throws ZMQCheckedException, InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        try (ZMQSocketFactory factory = new ZMQSocketFactory()) {
            StringBuilder received = new StringBuilder();
            CountDownLatch latch = new CountDownLatch(2);
            Thread t1 = ThreadBuilder.get().setDaemon(true).setTask(() -> {
                try (Socket pull = factory.getBuilder(Method.BIND, SocketType.PULL, rendezvous).setImmediate(false).build();
                     ZPoller poller = factory.getZPoller()){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                    
                    poller.register(pull, (s, m) -> {
                        logger.trace("Received message");
                        received.append(s.recvStr());
                        return true;
                    }, ZPoller.IN);
                    poller.register(pull, (s, m) -> {
                        logger.trace("Received error");
                        System.out.println(s.errno());
                        return true;
                    }, ZPoller.ERR);
                    int rc = poller.poll(3000);
                    logger.trace("Receive: {}", rc);
                    latch.countDown();
                } catch (ZMQCheckedException  ex) {
                    ex.printStackTrace();
                    received.setLength(0);
                }
            }).build(true);
            Thread t2 = ThreadBuilder.get().setDaemon(true).setTask(() -> {
                try (Socket push = factory.getBuilder(Method.CONNECT, SocketType.PUSH, rendezvous).setImmediate(false).build();
                     ZPoller poller = factory.getZPoller()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                    poller.register(push, (s, m) -> {
                        logger.trace("Sended message");
                        s.send("message");
                        return true;
                    }, ZPoller.OUT);
                    int rc = poller.poll(3000);
                    logger.trace("Sended: {}", rc);
                    latch.countDown();
                } catch (ZMQCheckedException  ex) {
                    ex.printStackTrace();
                    received.setLength(0);
                }
            }).build(true);
            latch.await(3, TimeUnit.SECONDS);
            Assert.assertEquals("message", received.toString());
            t1.join();
            t2.join();
        }
    }
}
