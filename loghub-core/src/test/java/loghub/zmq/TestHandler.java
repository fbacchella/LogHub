package loghub.zmq;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.zmq.ZMQHelper.Method;

public class TestHandler {

    private static Logger logger;

    @Rule(order = 1)
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Rule(order = 2)
    public final ZMQFactory tctxt = new ZMQFactory(testFolder, "secure");

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.ContextRule");
    }

    @Test(timeout = 5000)
    public void testHandlerRead() throws InterruptedException, ExecutionException, TimeoutException,
                                                 ZMQCheckedException {
        CompletableFuture<Boolean> wasStoped = new CompletableFuture<>();
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQHandler<byte[]> handler = new ZMQHandler.Builder<byte[]>()
                        .setZfactory(tctxt.getFactory())
                        .setSocketUrl(rendezvous)
                        .setMethod(Method.BIND)
                        .setType(SocketType.PULL)
                        .setLogger(logger)
                        .setName("testHandler")
                        .setReceive(Socket::recv)
                        .setMask(ZPoller.IN)
                        .setStopFunction(() -> wasStoped.complete(true))
                        .build();
        CountDownLatch latch = new CountDownLatch(1);
        FutureTask<byte[]> ft = new FutureTask<>(() -> {
            handler.start();
            byte[] msg = handler.dispatch(null);
            latch.countDown();
            return msg;
        });
        ThreadBuilder.get().setTask(ft).build(true);
        try (Socket s = tctxt.getFactory().getBuilder(Method.CONNECT, SocketType.PUSH, rendezvous).build()) {
            s.send("hello");
        }
        latch.await();
        handler.stopRunning();
        byte[] msg = ft.get(1, TimeUnit.SECONDS);
        Assert.assertEquals(true, wasStoped.get());
        Assert.assertEquals("hello", new String(msg, StandardCharsets.UTF_8));
    }

    @Test(timeout = 5000)
    public void testHandlerWrite() throws ZMQCheckedException, ExecutionException, InterruptedException {
        String rendezvous = "tcp://localhost:" + Tools.tryGetPort();
        ZMQHandler<byte[]> handler = new ZMQHandler.Builder<byte[]>()
                        .setZfactory(tctxt.getFactory())
                        .setSocketUrl(rendezvous)
                        .setMethod(Method.CONNECT)
                        .setType(SocketType.PUSH)
                        .setLogger(logger)
                        .setName("testHandler")
                        .setSend(Socket::send)
                        .setMask(ZPoller.OUT)
                        .build();
        FutureTask<Object> ft = new FutureTask<>(() -> {
            handler.start();
            Object m = handler.dispatch("hello".getBytes());
            handler.close();
            IllegalStateException ex = Assert.assertThrows(IllegalStateException.class, () -> handler.dispatch(null));
            Assert.assertEquals("ZMQ handler already stopped", ex.getMessage());
            return m;
        });
        ThreadBuilder.get().setTask(ft).build(true);
        try (Socket s = tctxt.getFactory().getBuilder(Method.BIND, SocketType.PULL, rendezvous).build()) {
            Assert.assertNull(ft.get());
            Assert.assertEquals("hello", s.recvStr());
        }
    }

}
