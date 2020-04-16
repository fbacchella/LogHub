package loghub;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.BiFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper.Method;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain=true)
public class ZMQSink implements Closeable {

    private static final Logger logger = LogManager.getLogger();

    public static class Builder {
        @Setter
        private String source = "tcp://localhost:2120";
        @Setter
        private SocketType type = SocketType.SUB;
        @Setter
        private int hwm = 1000;
        @Setter
        private Method method = Method.BIND;
        @Setter
        private String serverKey = null;
        @Setter
        private byte[] privateKey = null;
        @Setter
        private byte[] publicKey = null;
        @Setter
        private String security = null;
        @Setter
        private SmartContext ctx;
        @Setter
        private BiFunction<Socket, Integer, Boolean> localhandler = null;
        @Setter
        byte[] topic = null;

        public ZMQSink build() {
            return new ZMQSink(this);
        }
    }

    public volatile boolean running = false;
    public final Thread eventSink;
    private final ZMQHandler handler;
    private final String source;
    private Runnable handlerstopper = () -> {}; // Default to empty, don' fail on crossed start/stop

    private ZMQSink(Builder builder) {
        this.handler = new ZMQHandler.Builder()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.source)
                        .setMethod(builder.method)
                        .setType(builder.type)
                        .setTopic(builder.topic)
                        .setLogger(logger)
                        .setName("ZMQSink")
                        .setLocalHandler(builder.localhandler != null ? builder.localhandler: this::process)
                        .setMask(ZPoller.IN)
                        .setSecurity(builder.security)
                        .setPrivateKey(builder.privateKey)
                        .setPublicKey(builder.publicKey)
                        .setServerKeyToken(builder.serverKey)
                        .build();
        this.source = builder.source;
        eventSink = ThreadBuilder.get().setTask(() -> {
            ZMQSink.this.run();
        }).setDaemon(false).setName("Sink").build(true);
    }

    private void run() {
        handlerstopper = handler.getStopper();
        handler.run();
    }

    @Override
    public synchronized void close() throws IOException {
        handlerstopper.run();
        try {
            eventSink.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean process(Socket socket, int eventMask) {
        while ((socket.getEvents() & ZPoller.IN) != 0 && handler.isRunning()) {
            byte[] received = socket.recv();
            if (received == null) {
                Error error = Error.findByCode(socket.errno());
                logger.error("error with ZSocket {}: {}", source, error.getMessage());
                return false;
            }
        }
        return true;
    }

}
