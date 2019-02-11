package loghub.zmq;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import loghub.AbstractBuilder;
import loghub.zmq.ZMQHelper.ERRNO;
import loghub.zmq.ZMQHelper.Method;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.ZError;
import zmq.socket.Sockets;

@Accessors(chain=true)
public class ZMQHandler implements Closeable {

    public static class Builder extends AbstractBuilder<ZMQHandler> {
        @Setter
        String socketUrl;
        @Setter
        Sockets type;
        @Setter
        int hwm;
        @Setter
        Method method;
        @Setter
        Logger logger;
        @Setter
        String name;
        @Setter
        int mask;
        @Setter
        BiFunction<Socket, Integer, Boolean> localHandler;
        //ZPoller.EventsHandler localHandler;
        @Setter
        String security;
        @Setter
        byte[] serverKey;
        @Setter
        byte[] publicKey;
        @Setter
        byte[] privateKey;
        @Setter
        byte[] topic = null;
        @Setter
        Runnable stopFunction = () -> {};

        public Builder setServerKeyToken(String serverKeyToken) {
            this.serverKey = serverKeyToken != null ? ZMQHelper.parseServerIdentity(serverKeyToken) : null;
            return this;
        }

        @Override
        public ZMQHandler build() {
            return new ZMQHandler(this);
        }
    }

    private final Logger logger;
    private final String socketUrl;
    private final int mask;
    private final BiFunction<Socket, Integer, Boolean> localHandler;
    private final String pairId;
    // The stopFunction can be used to do a little cleaning and verifications before the join
    private final Runnable stopFunction;

    private volatile boolean running = false;
    private Supplier<Socket> makeSocket;
    private Socket socket;
    private Thread runningThread = null;
    private SmartContext ctx = null;

    private ZMQHandler(Builder builder) {
        this.logger = builder.logger;
        this.socketUrl = builder.socketUrl;
        this.mask = builder.mask;
        this.localHandler = builder.localHandler;
        this.pairId = builder.name + "/" + UUID.randomUUID();
        this.stopFunction = builder.stopFunction;
        makeSocket = () -> {
            runningThread = Thread.currentThread();
            ctx = SmartContext.getContext();
            Socket trysendsocket = null;
            try {
                trysendsocket = ctx.newSocket(builder.method, builder.type, socketUrl);
                if ("Curve".equals(builder.security)) {
                    logger.trace("Activating Curve security on a socket");
                    if (builder.privateKey == null && builder.publicKey == null) {
                        if (builder.serverKey != null) {
                            ctx.setCurveClient(trysendsocket, builder.serverKey);
                        } else if ("Curve".equals(builder.security)) {
                            ctx.setCurveServer(trysendsocket);
                        }
                    } else {
                        trysendsocket.setCurvePublicKey(builder.publicKey);
                        trysendsocket.setCurveSecretKey(builder.privateKey);
                        trysendsocket.setCurveServer(builder.serverKey == null);
                        if (builder.serverKey != null) {
                            trysendsocket.setCurveServerKey(builder.serverKey);
                        }
                    }
                } else if (builder.security != null) {
                    throw new IllegalArgumentException("Security  "+ builder.security + "not managed");
                }
            } catch (ZMQException e) {
                ZMQHelper.logZMQException(logger, "failed to start ZMQ handler " + socketUrl + ":", e);
                logger.catching(Level.DEBUG, e.getCause());
                trysendsocket = null;
            }
            if (builder.type == Sockets.SUB && builder.topic != null) {
                trysendsocket.subscribe(builder.topic);
            }
            return trysendsocket;
        };
    }

    public Runnable getStopper() {
        return () -> {
            stopRunning();
        };
    }

    public void run() {
        getSocket();
        running = true;
        Socket socketEndPair = ctx.newSocket(Method.BIND, Sockets.PAIR, "inproc://pair/" + pairId);
        try {
            ZPoller.EventsHandler stopsignal = new ZPoller.EventsHandler() {

                @Override
                public boolean events(Socket socket, int eventMask) {
                    logEvent(socket, eventMask);
                    if ((eventMask & ZPoller.ERR) != 0) {
                        ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                        logger.log(error.level, "error with ZSocket {}: {}", socketUrl, error.toStringMessage());
                    } else {
                        logger.debug("Received stop signal");
                        running = false;
                    }
                    return false;
                }

                @Override
                public boolean events(SelectableChannel channel, int events) {
                    throw new UnsupportedOperationException("Not registred for SelectableChannel");
                }

            };

            ZPoller.EventsHandler processevent = new ZPoller.EventsHandler() {

                @Override
                public boolean events(Socket socket, int eventMask) {
                    logEvent(socket, eventMask);
                    if ((eventMask & ZPoller.ERR) != 0) {
                        ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                        logger.log(error.level, "error with ZSocket {}: {}", socketUrl, error.toStringMessage());
                        return false;
                    } else if (localHandler != null) {
                        return localHandler.apply(socket, eventMask);
                    } else {
                        logger.error("No handler given");
                        running = false;
                        return false;
                    }
                }

                @Override
                public boolean events(SelectableChannel channel, int events) {
                    throw new UnsupportedOperationException("Not registred for SelectableChannel");
                }

            };
            while (isRunning()) {
                try (ZPoller zpoller = ctx.getZPoller()) {
                    logger.trace("Starting a poller {} {} {} {}", zpoller, socket, localHandler, mask);
                    zpoller.register(socket, processevent, mask | ZPoller.ERR);
                    zpoller.register(socketEndPair, stopsignal, ZPoller.ERR | ZPoller.IN);
                    while (isRunning() && zpoller.poll(-1L) > 0) {
                        logger.trace("Loopping the poller");
                    }
                } catch (IOException | ZError.IOException | ZError.InstantiationException | ZError.CtxTerminatedException e) {
                    logger.error("Error polling ZSocket {}: {}", socketUrl, e.getMessage());
                    logger.catching(Level.DEBUG, e);
                }
            }
        } finally {
            running = false;
            Optional.ofNullable(socket).ifPresent(ctx::close);
            Optional.ofNullable(socketEndPair).ifPresent(ctx::close);
            ctx.terminate();
        }
    }

    public void logEvent(Socket socket, int event) {
        logger.trace("receiving {} on {}", () -> "0b" + Integer.toBinaryString(8 + event).substring(1), () -> socket);
    }

    public void stopRunning() {
        logger.trace("Stop handling messages");
        if (running && ctx.isRunning()) {
            running = false;
            Socket stopStartPair = ctx.newSocket(Method.CONNECT, Sockets.PAIR, "inproc://pair/" + pairId);
            stopStartPair.send(new byte[] {});
            ctx.close(stopStartPair);
            logger.debug("Listening stopped");
            stopFunction.run();
            try {
                // Wait for end of processing the stop
                runningThread.join();
                logger.trace("Run stopped");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        return running && ! runningThread.isInterrupted() && ctx.isRunning();
    }

    @Override
    public void close() throws IOException {
        stopRunning();
    }

    /**
     * Can only be called from the the same thread that consume the socket
     * @return
     */
    public Socket getSocket() {
        socket = socket == null ? makeSocket.get() : socket;
        makeSocket = null;
        assert Thread.currentThread() == runningThread;
        return socket;
    }

}

