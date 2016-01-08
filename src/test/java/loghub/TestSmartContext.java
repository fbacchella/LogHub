package loghub;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public class TestSmartContext {

    private static final Logger logger = LogManager.getLogger();
    private final static AtomicLong KeyGenerator = new AtomicLong(0);


    @Test(timeout=1000)
    public void doTerminate() throws InterruptedException {

        final SmartContext context = SmartContext.getContext();

        Socket out = context.newSocket(Method.BIND, Type.PUSH, "inproc://in.TestPipeStep", 1, -1);
        Socket in = context.newSocket(Method.BIND, Type.PULL, "inproc://out.TestPipeStep", 1, -1);

        final Thread forward = new Thread() {

            @Override
            public void run() {
                Socket in = context.newSocket(Method.CONNECT, Type.PULL, "inproc://in.TestPipeStep", 1, -1);
                Socket out = context.newSocket(Method.CONNECT, Type.PUSH, "inproc://out.TestPipeStep", 1, -1);
                try {
                    for(byte[] msg: context.read(in)){
                        logger.debug("one received");
                        out.send(msg);
                    }
                    logger.debug("no more listening");
                } catch (org.zeromq.ZMQException e) {
                    logger.debug(e.getMessage());
                    logger.debug(e.getErrorCode());
                    logger.debug(e.getCause());
                    logger.catching(e);
                } catch (IOException e) {
                    logger.debug(e.getMessage());
                    logger.catching(e);
                } finally {
                    context.close(in);
                    context.close(out);
                }
            }
        };

        forward.start();

        long keyValue = KeyGenerator.getAndIncrement();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(keyValue);
        byte[] key = Arrays.copyOf(buffer.array(), 8);
        out.send(key);
        in.recv();

        context.close(in);
        context.close(out);
        SmartContext.terminate();
    }

}
