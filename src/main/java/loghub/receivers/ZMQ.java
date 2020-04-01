package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.Stats;
import loghub.configuration.Properties;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import lombok.Getter;
import lombok.Setter;
import zmq.ZError;

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


    private final ZMQHelper.Method method;
    @Getter
    private final String listen;

    private final SocketType type;
    @Getter
    private final String topic;
    @Getter
    private final int hwm ;
    @Getter
    private final String serverKey;
    @Getter
    private final String security;

    private Runnable handlerstopper = () -> {}; // Default to empty, don' fail on crossed start/stop
    private ZMQHandler handler;
    private byte[] databuffer;

    protected ZMQ(Builder builder) {
        super(builder);
        this.method = ZMQHelper.Method.valueOf(builder.method.toUpperCase(Locale.ENGLISH));
        this.listen = builder.listen;
        this.type = SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH));
        this.topic = builder.topic;
        this.hwm = builder.hwm;
        this.serverKey = builder.serverKey;
        this.security = builder.security;
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            this.handler = new ZMQHandler.Builder()
                            .setHwm(hwm)
                            .setSocketUrl(listen)
                            .setMethod(method)
                            .setType(type)
                            .setTopic(topic.getBytes(StandardCharsets.UTF_8))
                            .setServerKeyToken(serverKey)
                            .setLogger(logger)
                            .setName("zmqhandler:" + getReceiverName())
                            .setLocalHandler(this::process)
                            .setMask(ZPoller.IN)
                            .setSecurity(security)
                            .build();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            handlerstopper = handler.getStopper();
            int maxMsgSize = (int) handler.getSocket().getMaxMsgSize();
            if (maxMsgSize > 0 && maxMsgSize < 65535) {
                databuffer = new byte[maxMsgSize];
            } else {
                databuffer = new byte[65535];
            }
            handler.run();
        } catch (IllegalArgumentException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (RuntimeException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.ERROR, ex);
        } catch (ZMQCheckedException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    public boolean process(Socket socket, int eventMask) {
        while ((socket.getEvents() & ZPoller.IN) != 0 && handler.isRunning()) {
            int received;
            try {
                received = socket.recv(ZMQ.this.databuffer, 0, databuffer.length, 0);
                if (received < 0) {
                    Error error = Error.findByCode(socket.errno());
                    Stats.newReceivedError(String.format("error with ZSocket %s: %s", listen, error.getMessage()));
                } else {
                    decodeStream(ConnectionContext.EMPTY, databuffer, 0, received).forEach(this::send);
                }
            } catch (ZError.IOException | ZError.CtxTerminatedException | ZError.InstantiationException | ZMQException ex) {
                ZMQCheckedException cex = new ZMQCheckedException(ex);
                Stats.newReceivedError(String.format("error with ZSocket %s: %s", listen, cex.getMessage()));
            }
        }
        return true;
    }

    @Override
    public void close() {
        handlerstopper.run();
    }

    @Override
    public void stopReceiving() {
        handlerstopper.run();
    }

    public String getMethod() {
        return method.toString();
    }

    public String getType() {
        return type.name();
    }

    @Override
    public String getReceiverName() {
        return "ZMQ:" + listen;
    }

}
