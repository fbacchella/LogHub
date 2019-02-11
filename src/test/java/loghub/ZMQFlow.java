package loghub;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.ERRNO;
import loghub.zmq.ZMQHelper.Method;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.socket.Sockets;

@Accessors(chain=true)
public class ZMQFlow implements Closeable {

    private static final Logger logger = LogManager.getLogger();

    public static class Builder {
        @Setter
        private String destination = "tcp://localhost:2120";
        @Setter
        private Sockets type = Sockets.PUB;
        @Setter
        private int hwm = 1000;
        @Setter
        private Method method = Method.CONNECT;
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
        private Supplier<byte[]> source;
        @Setter
        private int msPause;
        @Setter
        private BiFunction<Socket, Integer, Boolean> localhandler = null;

        public ZMQFlow build() {
            return new ZMQFlow(this);
        }
    }

    public volatile boolean running = false;
    private final SmartContext ctx = SmartContext.getContext();
    private final Supplier<byte[]> source;
    private int msPause;
    private final Thread eventSource;
    private final ZMQHandler handler;

    private ZMQFlow(Builder builder) {
        this.handler = new ZMQHandler.Builder()
                        .setHwm(builder.hwm)
                        .setSocketUrl(builder.destination)
                        .setMethod(builder.method)
                        .setType(builder.type)
                        .setLogger(logger)
                        .setName("ZMQSink")
                        .setLocalHandler(null)
                        .setMask(ZPoller.IN)
                        .setSecurity(builder.security)
                        .setPrivateKey(builder.privateKey)
                        .setPublicKey(builder.publicKey)
                        .setServerKeyToken(builder.serverKey)
                        .build();

        this.source = builder.source;
        this.msPause = builder.msPause;
        eventSource = ThreadBuilder.get().setRunnable(() -> {
            ZMQFlow.this.run();
        }).setDaemon(false).setName("EventSource").build(true);
    }

    private void run() {
        logger.debug("flow started");
        Socket socket = null;
        try {
            socket = handler.getSocket();
            running = true;
            while (running && ctx.isRunning()) {
                byte[] message = source.get();
                synchronized (this) {
                    if (running && ctx.isRunning()) {
                        boolean sent = socket.send(message, ZMQ.DONTWAIT);
                        if (! sent && socket.errno() != ZMQHelper.ERRNO.EAGAIN.code) {
                            ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                            logger.error("send failed : {} {}", error, error.toStringMessage());
                        } else if (! sent ){
                            logger.debug("send: retry");
                        } else {
                            logger.trace("send: OK");
                        }
                    }
                }
                try {
                    Thread.sleep(msPause);
                } catch (InterruptedException e) {
                    running = false;
                    Thread.interrupted();
                }
            } 
        } finally {
            try {
                handler.close();
            } catch (IOException e) {
            }
            if (socket != null) {
                ctx.close(socket);
            }
            ctx.terminate();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        running = false;
        try {
            eventSource.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
