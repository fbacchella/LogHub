package loghub.senders;

import org.zeromq.ZMQ.Socket;

import loghub.BuilderClass;
import loghub.Event;
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
        ZMQHelper.Method method = ZMQHelper.Method.valueOf(builder.method.toUpperCase());
        Sockets type = Sockets.valueOf(builder.type.trim().toUpperCase());
        sendsocket = ctx.newSocket(method, type, builder.destination);
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
