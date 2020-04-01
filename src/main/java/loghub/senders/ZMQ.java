package loghub.senders;

import java.util.Locale;

import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import lombok.Getter;
import lombok.Setter;
import zmq.socket.Sockets;

@BuilderClass(ZMQ.Builder.class)
@CanBatch
public class ZMQ extends Sender {

    public static class Builder extends Sender.Builder<ZMQ> {
        @Setter @Getter
        private String destination = "tcp://localhost:2120";
        @Setter @Getter
        private String type = Sockets.PUB.name();
        @Setter @Getter
        private int hwm = 1000;
        @Setter @Getter
        private String method = ZMQHelper.Method.BIND.name();
        @Setter  @Getter
        private String serverKey = null;
        @Setter  @Getter
        private String security = null;

        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final ZMQHandler handler;
    private Runnable handlerstopper = () -> {}; // Default to empty, don' fail on crossed start/stop
    //Interrupt is only allowed outside of ZMQ poll, this flag protect that
    private volatile boolean canInterrupt;

    public ZMQ(Builder builder) {
        super(builder);
        this.handler = new ZMQHandler.Builder()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.destination)
                        .setMethod(Method.valueOf(builder.getMethod().toUpperCase(Locale.ENGLISH)))
                        .setType(SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH)))
                        .setSecurity(builder.security)
                        .setServerKeyToken(builder.serverKey)
                        .setLogger(logger)
                        .setName(getName())
                        .setLocalHandler(this::process)
                        .setMask(ZPoller.OUT)
                        .setStopFunction(() -> {
                            if (canInterrupt) {
                                ZMQ.this.interrupt();
                            }
                        })
                        .build();
    }

    @Override
    public boolean configure(Properties properties) {
        if (handler == null) {
            return false;
        } else {
            return super.configure(properties);
        }
    }

    @Override
    public void run() {
        handlerstopper = handler.getStopper();
        handler.run();
    }

    @Override
    public void stopSending() {
        handlerstopper.run();
        super.stopSending();
    }

    public boolean send(Event event) {
        throw new UnsupportedOperationException("No direct send");
    }

    @Override
    public String getSenderName() {
        return "ZMQ";
    }

    public boolean process(Socket socket, int eventMask) {
        try {
            while (handler.isRunning() && (socket.getEvents() & ZPoller.OUT) != 0) {
                canInterrupt = true;
                Event event = getNext();
                byte[] msg = getEncoder().encode(event);
                canInterrupt = false;
                boolean sent = socket.send(msg);
                processStatus(event, sent);
            }
            return true;
        } catch (InterruptedException e) {
            // Don't rethrow interrupt, ZMQ don't like them
            // The false value will be enough to stop processing
            return false;
        } finally {
            canInterrupt = false;
        }
    }

    @Override
    public void close() {
        stopSending();
    }

}
