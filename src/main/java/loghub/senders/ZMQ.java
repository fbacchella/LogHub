package loghub.senders;

import org.zeromq.ZMQ.Socket;

import org.zeromq.ZMQException;

import loghub.BuilderClass;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper;
import lombok.Setter;
import zmq.socket.Sockets;

@BuilderClass(ZMQ.Builder.class)
public class ZMQ extends Sender {

    public static class Builder extends Sender.Builder<ZMQ> {
        @Setter
        private String destination = "tcp://localhost:2120";
        @Setter
        private String type = Sockets.PUB.name();
        @Setter
        private int hwm = 1000;
        @Setter
        private String method = ZMQHelper.Method.BIND.name();
        private byte[] serverKey = null;
        @Setter
        private String security = null;

        public void setServerKey(String key) {
            this.serverKey = ZMQHelper.parseServerIdentity(key);
        }

        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Socket sendsocket;
    private final SmartContext ctx = SmartContext.getContext();
    private volatile boolean running = false;

    public ZMQ(Builder builder) {
        super(builder);
        Socket trysendsocket = null;
        try {
            ZMQHelper.Method method = ZMQHelper.Method.valueOf(builder.method.toUpperCase());
            Sockets type = Sockets.valueOf(builder.type.trim().toUpperCase());
            trysendsocket = ctx.newSocket(method, type, builder.destination);
            if ("Curve".equals(builder.security) && builder.serverKey != null) {
                ctx.setCurveClient(trysendsocket, builder.serverKey);
            } else if ("Curve".equals(builder.security)) {
                ctx.setCurveServer(trysendsocket);
            }
        } catch (ZMQException e) {
            ZMQHelper.logZMQException(logger, "", e);
        }
        sendsocket = trysendsocket;
    }

    @Override
    public boolean configure(Properties properties) {
        if (sendsocket == null) {
            return false;
        } else {
            return super.configure(properties);
        }
    }

    @Override
    public void run() {
        running = true;
        super.run();
    }

    @Override
    public void stopSending() {
        running = false;
        super.stopSending();
    }

    @Override
    public boolean send(Event event) {
        byte[] msg = getEncoder().encode(event);
        if (running) {
            sendsocket.send(msg);
            return true;
        } else {
            ctx.close(sendsocket);
            return false;
        }
    }

    @Override
    public String getSenderName() {
        return "ZMQ";
    }

}
