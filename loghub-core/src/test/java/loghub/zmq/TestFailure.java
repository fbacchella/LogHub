package loghub.zmq;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;

import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.ZMQFactory;
import loghub.zmq.ZMQHelper.Method;

public class TestFailure {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq", "loghub.ZMQFactory");
    }

    @Rule()
    public ZMQFactory tctxt = new ZMQFactory();

    @Test(expected = ZMQCheckedException.class)
    public void reuse() throws ZMQCheckedException {
        ZMQSocketFactory ctx = tctxt.getFactory();
        String endpoint = "tcp://localhost:" + Tools.tryGetPort();
        try (
             Socket s1 = ctx.getBuilder(Method.BIND, SocketType.PUB, endpoint).build();
             Socket s2 = ctx.getBuilder(Method.BIND, SocketType.PUB, endpoint).build()
            ){
            // empty
        } catch (ZMQCheckedException ex) {
            Assert.assertEquals(Error.EADDRINUSE, ex.getError());
            logger.debug(ex);
            throw ex;
        }
    }

    @Test(expected = ZMQCheckedException.class)
    public void unsupported() throws ZMQCheckedException {
        ZMQSocketFactory ctx = tctxt.getFactory();
        String endpoint = "pgm://localhost:" + Tools.tryGetPort();
        try (
             Socket s1 = ctx.getBuilder(Method.BIND, SocketType.PUB, endpoint).build();
             Socket s2 = ctx.getBuilder(Method.BIND, SocketType.SUB, endpoint).build()) {
            // empty
        } catch (ZMQCheckedException ex) {
            Assert.assertEquals(Error.EPROTONOSUPPORT, ex.getError());
            logger.debug(ex);
            throw ex;
        }
    }

    @Test
    public void interrupted() throws InterruptedException {
        ZMQSocketFactory ctx = tctxt.getFactory();
        String endpoint = "tcp://localhost:" + Tools.tryGetPort();
        CountDownLatch latch = new CountDownLatch(1);
        FutureTask<Object> blocked = new FutureTask<>(() -> {
            try (
                 Socket s1 = ctx.getBuilder(Method.BIND, SocketType.SUB, endpoint).build()){
                latch.countDown();
                return s1.recv();
            } catch (UncheckedZMQException ex) {
                logger.trace(ex);
                throw new ZMQCheckedException(ex);
            } finally {
                logger.trace("finished");
                ctx.close();
            }
        });
        Thread t = ThreadBuilder.get().setTask(blocked::run).setDaemon(true).build(true);

        latch.await();
        Thread.sleep(100);
        t.interrupt();
        Throwable ex = Assert.assertThrows(ZMQCheckedException.class, () -> {
            try {
                blocked.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
        Assert.assertEquals(Error.EINTR, ((ZMQCheckedException) ex).getError());
        ctx.close();
    }

}
