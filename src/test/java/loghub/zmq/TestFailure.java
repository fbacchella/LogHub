package loghub.zmq;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;

import loghub.LogUtils;
import loghub.ThreadBuilder;
import loghub.Tools;
import loghub.zmq.ZMQHelper.Method;

public class TestFailure {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.zmq");
    }

    @Test(expected = ZMQCheckedException.class)
    public void reuse() throws ZMQCheckedException {
        SmartContext ctx = SmartContext.getContext();
        String endpoint = "tcp://localhost:" + Tools.tryGetPort();
        Socket s1 = null;
        Socket s2 = null;
        try {
            s1 = ctx.newSocket(Method.BIND, SocketType.PUB, endpoint);
            s2 = ctx.newSocket(Method.BIND, SocketType.PUB, endpoint);
        } catch (ZMQCheckedException ex) {
            Assert.assertEquals(Error.EADDRINUSE, ex.getError());
            logger.debug(ex);
            throw ex;
        } finally {
            Optional.ofNullable(s1).ifPresent(i -> ctx.close(i));
            Optional.ofNullable(s2).ifPresent(i -> ctx.close(i));
            ctx.terminate();
        }
    }

    @Test(expected = ZMQCheckedException.class)
    public void unsupported() throws ZMQCheckedException {
        SmartContext ctx = SmartContext.getContext();
        String endpoint = "pgm://localhost:" + Tools.tryGetPort();
        Socket s1 = null;
        Socket s2 = null;
        try {
            s1 = ctx.newSocket(Method.BIND, SocketType.PUB, endpoint);
            s2 = ctx.newSocket(Method.BIND, SocketType.PUB, endpoint);
        } catch (ZMQCheckedException ex) {
            Assert.assertEquals(Error.EPROTONOSUPPORT, ex.getError());
            logger.debug(ex);
            throw ex;
        } finally {
            Optional.ofNullable(s1).ifPresent(i -> ctx.close(i));
            Optional.ofNullable(s2).ifPresent(i -> ctx.close(i));
            ctx.terminate();
        }
    }

    @Test
    public void interrupted() throws ZMQCheckedException, InterruptedException, ExecutionException {
        SmartContext ctx = SmartContext.getContext();
        String endpoint = "tcp://localhost:" + Tools.tryGetPort();
        CountDownLatch latch = new CountDownLatch(1);
        FutureTask<Exception> blocked = new FutureTask<>(() -> {
            Socket s1 = null;
            try {
                s1 = ctx.newSocket(Method.BIND, SocketType.SUB, endpoint);
                latch.countDown();
                s1.recv();
                Optional.ofNullable(s1).ifPresent(i -> ctx.close(i));
                return null;
            } catch (ZMQCheckedException e) {
                logger.debug(e);
                return e;
            } catch (RuntimeException ex) {
                try {
                    ZMQCheckedException.raise(ex);
                } catch (Exception sex) {
                    return sex;
                }
                return null;
            }
        });
        try {
            Thread t = ThreadBuilder.get().setTask(blocked).setDaemon(true).build(true);
            latch.await();
            Thread.sleep(100);
            t.interrupt();
            Exception ex = blocked.get();
            Assert.assertEquals(ZMQCheckedException.class, ex.getClass());
            Assert.assertEquals(Error.EINTR, ((ZMQCheckedException) ex).getError());
            logger.debug("catched exception", ex);
        } finally {
            ctx.terminate();
        }
    }

}
