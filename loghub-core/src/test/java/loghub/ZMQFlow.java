package loghub;

import java.io.IOException;
import java.security.KeyStore.PrivateKeyEntry;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.io.mechanism.Mechanisms;

@Accessors(chain=true)
public class ZMQFlow extends Thread implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger();

    @Setter
    public static class Builder {
        private String destination = "tcp://localhost:2120";
        private SocketType type = SocketType.PUB;
        private int hwm = 1000;
        private Method method = Method.CONNECT;
        private String serverKey = null;
        PrivateKeyEntry keyEntry = null;
        private Mechanisms security = Mechanisms.NULL;
        private ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;
        private Supplier<byte[]> source;
        private int msPause;
        private ZMQSocketFactory zmqFactory = null;

        public ZMQFlow build() {
            return new ZMQFlow(this);
        }
    }

    public volatile boolean running = false;
    private final Supplier<byte[]> source;
    private final int msPause;
    private final ZMQHandler<byte[]> handler;

    private ZMQFlow(Builder builder) {
        this.handler = new ZMQHandler.Builder<byte[]>()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.destination)
                        .setMethod(builder.method)
                        .setType(builder.type)
                        .setLogger(logger)
                        .setName("ZMQFlow")
                        .setSend(Socket::send)
                        .setMask(ZPoller.OUT | ZPoller.ERR)
                        .setSecurity(builder.security)
                        .setZapHandler(builder.zapHandler)
                        .setKeyEntry(builder.keyEntry)
                        .setServerPublicKeyToken(builder.serverKey)
                        .setZfactory(builder.zmqFactory)
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
    public synchronized void close() {
        running = false;
        handler.stopRunning();
    }

}
