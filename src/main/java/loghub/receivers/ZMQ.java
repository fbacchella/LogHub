package loghub.receivers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import lombok.Setter;

@Blocking
@BuilderClass(ZMQ.Builder.class)
public class ZMQ extends Receiver {

    public static class Builder extends Receiver.Builder<ZMQ> {
        @Setter
        String method = ZMQHelper.Method.BIND.name();
        @Setter
        String listen = "tcp://localhost:2120";
        @Setter
        int hwm= 1000;
        @Setter
        String type = SocketType.SUB.name();
        @Setter
        String serverKey = null;
        @Setter
        String security = null;
        @Setter
        String topic = "";
        @Override
        public ZMQ build() {
            return new ZMQ(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private ZMQHandler.Builder<byte[]> hbuilder;
    private ZMQHandler<byte[]> handler;
    private final String listen;

    protected ZMQ(Builder builder) {
        super(builder);
        hbuilder = new ZMQHandler.Builder<>();
        hbuilder.setHwm(builder.hwm)
                .setSocketUrl(builder.listen)
                .setMethod(Method.valueOf(builder.method.toUpperCase(Locale.ENGLISH)))
                .setType(SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH)))
                .setTopic(builder.topic.getBytes(StandardCharsets.UTF_8))
                .setServerPublicKeyToken(builder.serverKey)
                .setLogger(logger)
                .setName("zmqhandler:" + getReceiverName())
                .setReceive(Socket::recv)
                .setMask(ZPoller.IN)
                .setSecurity(builder.security)
                ;
        this.listen = builder.listen;
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            this.handler = hbuilder
                            .setZfactory(properties.zSocketFactory)
                            .build();
            this.hbuilder = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            handler.start();
            while (handler.isRunning()) {
                byte[] message = handler.dispatch(null);
                if (message != null) {
                    decodeStream(ConnectionContext.EMPTY, message).forEach(this::send);
                }
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (ZMQCheckedException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (Throwable ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } finally {
            handler.close();
        }
    }

    @Override
    public void close() {
        try {
            handler.stopRunning();
        } catch (IOException | ZMQCheckedException ex) {
            logger.error("Failed ZMQ socket close : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public void stopReceiving() {
        try {
            handler.stopRunning();
        } catch (IOException | ZMQCheckedException ex) {
            logger.error("Failed receiver ZMQ stop : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public void interrupt() {
        handler.interrupt(this, super::interrupt);
    }

    @Override
    public String getReceiverName() {
        return "ZMQ:" + listen;
    }

}
