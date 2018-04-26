package loghub;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper.Method;
import zmq.socket.Sockets;

public class TestSmartContext {

    private static final Logger logger = LogManager.getLogger();
    private final static AtomicLong KeyGenerator = new AtomicLong(0);

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.zmq");
    }

    @Test(timeout=2000)
    public void doTerminate() throws InterruptedException, ExecutionException {
        SmartContext context = SmartContext.getContext();

        FutureTask<Boolean> doForward = new FutureTask<>(() -> {
            try (Socket in = context.newSocket(Method.CONNECT, Sockets.PULL, "inproc://in.TestPipeStep", 1, -1);
                    Socket out = context.newSocket(Method.CONNECT, Sockets.PUSH, "inproc://out.TestPipeStep", 1, -1);) {
                for(byte[] msg: context.read(in)){
                    logger.debug("one received");
                    out.send(msg);
                }
                logger.debug("no more listening");
                return true;
            } catch (org.zeromq.ZMQException e) {
                logger.debug(e.getMessage());
                logger.debug(e.getErrorCode());
                logger.debug(e.getCause());
                logger.catching(e);
                return false;
            } catch (IOException e) {
                logger.debug(e.getMessage());
                logger.catching(e);
                return false;
            }
        });
        ThreadBuilder.get(Boolean.class).setCallable(doForward).setDaemon(false).build(true);
        try (Socket out = context.newSocket(Method.BIND, Sockets.PUSH, "inproc://in.TestPipeStep", 1, -1);
                Socket in = context.newSocket(Method.BIND, Sockets.PULL, "inproc://out.TestPipeStep", 1, -1);
                ) {
            long keyValue = KeyGenerator.getAndIncrement();
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(keyValue);
            byte[] key = Arrays.copyOf(buffer.array(), 8);
            out.send(key);
            in.recv();
            Assert.assertTrue(SmartContext.getContext().terminate().get());
            Assert.assertTrue(doForward.get());
        }
    }

}
