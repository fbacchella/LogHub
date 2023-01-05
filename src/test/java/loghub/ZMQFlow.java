package loghub;

import java.io.IOException;
import java.security.KeyStore.PrivateKeyEntry;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.io.mechanism.Mechanisms;

@Accessors(chain=true)
public class ZMQFlow extends Thread implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger();

    public static class Builder {
        @Setter
        private String destination = "tcp://localhost:2120";
        @Setter
        private SocketType type = SocketType.PUB;
        @Setter
        private int hwm = 1000;
        @Setter
        private Method method = Method.CONNECT;
        @Setter
        private String serverKey = null;
        @Setter
        PrivateKeyEntry keyEntry = null;
        @Setter
        private Mechanisms security = Mechanisms.NULL;
        @Setter 
        private Supplier<byte[]> source;
        @Setter
        private int msPause;
        @Setter
        private BiFunction<Socket, Integer, Boolean> localhandler = null;
        @Setter
        private ZMQSocketFactory zmqFactory = null;

        public ZMQFlow build() {
            return new ZMQFlow(this);
        }
    }

    public volatile boolean running = false;
    private final Supplier<byte[]> source;
    private int msPause;
    private final ZMQHandler<byte[]> handler;

    private ZMQFlow(Builder builder) {
        this.handler = new ZMQHandler.Builder<byte[]>()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.destination)
                        .setMethod(builder.method)
                        .setType(builder.type)
                        .setLogger(logger)
                        .setName("ZMQSink")
                        .setSend(Socket::send)
                        .setMask(ZPoller.OUT)
                        .setSecurity(builder.security)
                        .setKeyEntry(builder.keyEntry)
                        .setServerPublicKeyToken(builder.serverKey)
                        .setZfactory(builder.zmqFactory)
                        .setEventCallback(ZMQHelper.getEventLogger(logger))
                        .build();

        this.source = builder.source;
        this.msPause = builder.msPause;
        setDaemon(true);
        setName("EventSource");
        start();
    }

    public void run() {
        logger.debug("flow started");
        try {
            handler.start();
            running = true;
            while (running) {
                byte[] message = source.get();
                handler.dispatch(message);
                try {
                    Thread.sleep(msPause);
                } catch (InterruptedException e) {
                    running = false;
                    Thread.interrupted();
                }
            } 
        } catch (ZMQCheckedException ex) {
            logger.error("Failed handler dispatch", ex);
        } finally {
            handler.close();
        }
    }

    @Override
    public synchronized void close() throws IOException, ZMQCheckedException {
        running = false;
        handler.stopRunning();
    }

}
