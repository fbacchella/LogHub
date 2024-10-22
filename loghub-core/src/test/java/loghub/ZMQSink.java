package loghub;

import java.security.KeyStore.PrivateKeyEntry;
import java.util.function.Function;

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
public class ZMQSink<M> extends Thread implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger();

    @Setter
    public static class Builder<M> {
        private String source = "tcp://localhost:2120";
        private SocketType type = SocketType.SUB;
        private int hwm = 1000;
        private Method method = Method.BIND;
        private String serverKey = null;
        PrivateKeyEntry keyEntry = null;
        private Mechanisms security = Mechanisms.NULL;
        private ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;
        private ZMQSocketFactory zmqFactory = null;
        Function<Socket, M> receive = null;
        byte[] topic = null;

        private Builder() {}

        public ZMQSink<M> build() {
            return new ZMQSink<>(this);
        }
    }

    public static <M> Builder<M> getBuilder() {
        return new Builder<>();
    }

    private final ZMQHandler<M> handler;

    private ZMQSink(Builder<M> builder) {
        handler = new ZMQHandler.Builder<M>()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.source)
                        .setMethod(builder.method)
                        .setType(builder.type)
                        .setTopic(builder.topic)
                        .setLogger(logger)
                        .setName("ZMQSink")
                        .setReceive(builder.receive)
                        .setMask(ZPoller.IN)
                        .setSecurity(builder.security)
                        .setZapHandler(builder.zapHandler)
                        .setKeyEntry(builder.keyEntry)
                        .setServerPublicKeyToken(builder.serverKey)
                        .setZfactory(builder.zmqFactory)
                        .build();
        setDaemon(true);
        setName("Sink");
        start();
    }

    public void run() {
        try {
            handler.start();
            logger.debug("Sink started");
            while (handler.isRunning()) {
                logger.trace("Sink loop");
                handler.dispatch(null);
            }
        } catch (ZMQCheckedException ex) {
            logger.error("Failed handler dispatch", ex);
        } finally {
            handler.close();
        }
    }

    @Override
    public synchronized void close() {
       handler.stopRunning();
    }

    @Override
    public void interrupt() {
        handler.interrupt(this, super::interrupt);
    }

}
