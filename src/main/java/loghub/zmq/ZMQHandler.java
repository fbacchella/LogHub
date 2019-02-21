package loghub.zmq;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.zmq.ZMQHelper.Method;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.ZError;

@Accessors(chain=true)
public class ZMQHandler implements Closeable {

    public static class Builder extends AbstractBuilder<ZMQHandler> {
        @Setter
        String socketUrl;
        @Setter
        SocketType type;
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
        @Setter
        Consumer<String> injectError;

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
    private final Consumer<String> injectError;

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
        this.injectError = builder.injectError;
        makeSocket = () -> {
            runningThread = Thread.currentThread();
            ctx = SmartContext.getContext();
            Socket trysendsocket = null;
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
            if (builder.type == SocketType.SUB && builder.topic != null) {
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
        running = true;
        Socket socketEndPair = null;
        try {
            getSocket();
            socketEndPair = ctx.newSocket(Method.BIND, SocketType.PAIR, "inproc://pair/" + pairId);
            ZPoller.EventsHandler stopsignal = new ZPoller.EventsHandler() {
                @Override
                public boolean events(Socket socket, int eventMask) {
                    logEvent(socket, eventMask);
                    if ((eventMask & ZPoller.ERR) != 0) {
                        Error error = Error.findByCode(socket.errno());
                        logger.error("error with ZSocket {}: {}", socketUrl, error.getMessage());
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
                        Error error = Error.findByCode(socket.errno());
                        injectError.accept(String.format("error with ZSocket %s: %s", socketUrl, error.getMessage()));
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
                } catch (IOException e) {
                    Error error = Error.findByCode(socket.errno());
                    injectError.accept(String.format("error with ZSocket %s: %s", socketUrl, error.getMessage()));
                } catch (ZError.IOException | ZError.InstantiationException | ZError.CtxTerminatedException e) {
                    ZMQCheckedException.logZMQException(logger, "failure when polling a ZSocket: {}", e);
                }
            }
        } catch (RuntimeException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.ERROR, ex);
        } catch (ZMQCheckedException ex) {
            logger.error("Failed ZMQ processing : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
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
            Socket stopStartPair = ctx.newSocket(Method.CONNECT, SocketType.PAIR, "inproc://pair/" + pairId);
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
     * @throws ZMQCheckedException if socket creation failed
     */
    public Socket getSocket() throws ZMQCheckedException {
        try {
            socket = socket == null ? makeSocket.get() : socket;
            makeSocket = null;
            assert Thread.currentThread() == runningThread;
            return socket;
        } catch (RuntimeException e) {
            ZMQCheckedException.raise(e);
            // Not reached ZMQCheckedException always throws an exception
            return null;
        }
    }

}
