package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.Start;
import loghub.configuration.Properties;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZmqConnectionContext;
import lombok.Setter;
import zmq.Msg;
import zmq.io.mechanism.Mechanisms;

@Blocking
@BuilderClass(ZMQ.Builder.class)
public class ZMQ extends Receiver<ZMQ, ZMQ.Builder> {

    public static class Builder extends Receiver.Builder<ZMQ, ZMQ.Builder> {
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
        Mechanisms security = null;
        @Setter
        String topic = "";
        @Override
        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private ZMQHandler.Builder<Msg> hbuilder;
    private ZMQHandler<Msg> handler;
    private final String listen;
    private final Mechanisms security;

    protected ZMQ(Builder builder) {
        super(builder);
        this.listen = builder.listen;
        this.security = builder.security;
        hbuilder = new ZMQHandler.Builder<>();
        hbuilder.setHwm(builder.hwm)
                .setSocketUrl(builder.listen)
                .setMethod(Method.valueOf(builder.method.toUpperCase(Locale.ENGLISH)))
                .setType(SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH)))
                .setTopic(builder.topic.getBytes(StandardCharsets.UTF_8))
                .setSecurity(security)
                .setServerPublicKeyToken(builder.serverKey)
                .setLogger(logger)
                .setName("zmqhandler/" + listen.replaceFirst("://", "/").replace(':', '/').replaceFirst("\\*", "0.0.0.0"))
                .setReceive(s -> s.base().recv(0))
                .setMask(ZPoller.IN)
                ;
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
                Msg msg = handler.dispatch(null);
                if (msg == null) {
                    continue;
                }
                byte[] message = msg.data();
                if (message != null) {
                    decodeStream(new ZmqConnectionContext(msg, security), message).forEach(this::send);
                }
            }
        } catch (ZMQCheckedException | IllegalArgumentException ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (Throwable ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            if (Helpers.isFatal(ex)) {
                Start.fatalException(ex);
                logger.catching(Level.FATAL, ex);
            } else {
                logger.catching(Level.ERROR, ex);
            }
        } finally {
            handler.close();
        }
    }

    @Override
    public void close() {
        try {
            handler.stopRunning();
        } catch (ZMQCheckedException ex) {
            logger.error("Failed ZMQ socket close : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public void stopReceiving() {
        try {
            handler.stopRunning();
        } catch (ZMQCheckedException ex) {
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
        return "ZMQ_" + listen.replaceFirst("://", "/").replace(':', '/').replaceFirst("\\*", "0.0.0.0");
    }

}
