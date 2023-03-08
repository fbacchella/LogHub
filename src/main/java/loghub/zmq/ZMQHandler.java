package loghub.zmq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.cert.Certificate;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory.SocketBuilder;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import lombok.Setter;
import lombok.experimental.Accessors;
import zmq.io.mechanism.Mechanisms;

@Accessors(chain=true)
public class ZMQHandler<M> implements AutoCloseable {

    @FunctionalInterface
    private interface PrepareFactory {
        void run() throws ZMQCheckedException;
    }

    public static class Builder<M> extends AbstractBuilder<ZMQHandler<M>> {
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
        Mechanisms security = Mechanisms.NULL;
        @Setter
        Certificate serverPublicKey;
        @Setter
        PrivateKeyEntry keyEntry = null;
        @Setter
        byte[] topic = null;
        @Setter
        Runnable stopFunction = () -> {};
        @Setter
        Consumer<String> injectError;
        @Setter
        ZMQSocketFactory zfactory = null;
        @Setter
        CountDownLatch latch = null;
        @Setter
        Function<Socket, M> receive = null;
        @Setter
        BiFunction<Socket, M, Boolean> send = null;
        @Setter
        ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;

        public Builder<M> setServerPublicKeyToken(String serverKeyToken) {
            this.serverPublicKey = serverKeyToken != null ? ZMQHelper.parseServerIdentity(serverKeyToken) : null;
            return this;
        }

        @Override
        public ZMQHandler<M> build() {
            return new ZMQHandler<>(this);
        }
    }

    private final Logger logger;
    private final String socketUrl;
    private final int mask;
    private final Function<Socket, M> receive;
    private final BiFunction<Socket, M, Boolean> send;
    // The stopFunction can be used to do a little cleaning and verifications before the join
    private final Runnable stopFunction;
    private final Pipe notificationPipe;

    private volatile boolean running = false;
    private PrepareFactory makeThreadLocal;
    private Socket socket;
    private Thread runningThread = null;
    private ZPoller pooler;
    // Settings of the factory can be delayed
    @Setter
    private ZMQSocketFactory zfactory;
    //Interrupt is only allowed outside ZMQ poll or socket options, this flag protect that
    private volatile boolean canInterrupt = true;

    private ZMQHandler(Builder<M> builder) {
        this.logger = builder.logger;
        this.socketUrl = builder.socketUrl;
        this.mask = builder.mask;
        this.stopFunction = builder.stopFunction;
        this.zfactory = builder.zfactory;
        this.send = builder.send;
        this.receive = builder.receive;
        try {
            this.notificationPipe = Pipe.open();
            this.notificationPipe.sink().configureBlocking(false);
            this.notificationPipe.source().configureBlocking(false);
        } catch (IOException ex) {
            throw new IllegalStateException("Internal signaling pipe can't be created: " + Helpers.resolveThrowableException(ex), ex);
        }
        // Socket creation is delayed
        // So the socket is created in the using thread
        makeThreadLocal = () -> {
            makeThreadLocal = null;
            runningThread = Thread.currentThread();
            SocketBuilder sbuilder = zfactory.getBuilder(builder.method, builder.type, socketUrl).setTopic(builder.topic).setImmediate(false);
            sbuilder.setLoggerMonitor(builder.name, builder.logger);
            Mechanisms security = builder.security;
            sbuilder.setSecurity(security);
            switch (security) {
            case CURVE: 
                logger.debug("Activating Curve security on socket {}", socketUrl);
                if (zfactory.getZapService() != null) {
                    sbuilder.setZapDomain(builder.name);
                    zfactory.getZapService().addFilter(builder.name, builder.zapHandler.get(zfactory, security));
                }
                sbuilder.setCurveKeys(Optional.ofNullable(builder.keyEntry).orElse(zfactory.getKeyEntry()));
                if (builder.serverPublicKey != null) {
                    sbuilder.setServerPublicKey(builder.serverPublicKey);
                }
                break;
            case NULL:
                logger.debug("Activating Null security on socket {}", socketUrl);
                break;
            default:
                throw new IllegalArgumentException("Security  "+ builder.security + "not handled");
            }
            socket = sbuilder.build();
            pooler = zfactory.getZPoller();
            pooler.register(socket, mask | ZPoller.ERR);
            pooler.register(notificationPipe.source(), ZPoller.IN | ZPoller.ERR);
            running = true;
            if (builder.latch != null) {
                builder.latch.countDown();
            }
        };
    }

    public void start() throws ZMQCheckedException {
        makeThreadLocal.run();
        makeThreadLocal = null;
    }

    public M dispatch(M message) throws ZMQCheckedException {
        assert Thread.currentThread() == runningThread;
        if (! isRunning()) {
            throw new IllegalStateException("ZMQ handler already stopped");
        }
        logger.trace("One dispatch loop");
        M received = null;
        // Loop until an event is received in the main socket.
        while (isRunning()) {
            try {
                canInterrupt = false;
                // Keep the usage of Thread.interrupted(), jeromq break with thread interruption
                if (Thread.interrupted()) {
                    running = false;
                    break;
                }
                int count = pooler.poll(-1L);
                canInterrupt = true;
                if (count > 0) {
                    int sEvents = ZMQCheckedException.checkCommand(socket.getEvents(),
                                                                   socket);
                    // Received an error, end processing
                    if ((sEvents & ZPoller.ERR) != 0) {
                        logEvent("Error on socket", socket, sEvents);
                        throw new ZMQCheckedException(socket.errno());
                    } else if (pooler.isError(notificationPipe.source())) {
                        running = false;
                        logEvent("Error on notification pipe", notificationPipe.source());
                    } else if (pooler.isReadable(notificationPipe.source())) {
                        running = false;
                        // An end signal event
                        logEvent("Stop signal", notificationPipe.source());
                        readNotification();
                        break;
                    } else if (message != null && (sEvents & ZPoller.OUT) != 0) {
                        logEvent("Message sent", socket, sEvents);
                        if (Boolean.FALSE.equals(send.apply(socket, message))) {
                            throw new ZMQCheckedException(socket.errno());
                        }
                        break;
                    } else if (message == null && (sEvents & ZPoller.IN) != 0) {
                        logEvent("Message received", socket, sEvents);
                        received = receive.apply(socket);
                        if (received == null) {
                            throw new ZMQCheckedException(socket.errno());
                        } else {
                            break;
                        }
                    }
               }
            } catch (UncheckedZMQException ex) {
                throw new ZMQCheckedException(ex);
            } finally {
                canInterrupt = true;
            }
        }
        return received;
    }

    private void readNotification() {
        try {
            ByteBuffer inBuffer = ByteBuffer.allocate(1);
            notificationPipe.source().read(inBuffer);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed notification: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    private void writeNotification() {
        try {
            ByteBuffer inBuffer = ByteBuffer.allocate(1);
            notificationPipe.sink().write(inBuffer);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed notification: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    public void logEvent(String prefix, Socket socket, int event) {
        logger.trace("{}: receiving {}{}{} on {}",
                     () -> prefix,
                     () -> (event & ZPoller.ERR) != 0 ? "E" : ".",
                     () -> (event & ZPoller.OUT) != 0 ? "O" : ".",
                     () -> (event & ZPoller.IN) != 0  ? "I" : ".",
                     () -> socket);
    }

    public void logEvent(String prefix, SelectableChannel channel) {
        logger.trace("{}: receiving {}{}{} on {}",
                () -> prefix,
                () -> pooler.isError(channel) ? "E" : ".",
                () -> pooler.isWritable(channel) ? "O" : ".",
                () -> pooler.isReadable(channel)  ? "I" : ".",
                () -> channel instanceof WritableByteChannel ? "SinkChannel" : "SourceChannel");
    }

    public boolean isRunning() {
        return running && ! runningThread.isInterrupted();
    }

    @Override
    public void close() {
        assert Thread.currentThread() == runningThread;
        running = false;
   }

    public void close(Socket s) {
        boolean interrupted;
        try {
            canInterrupt = false;
            // Keep Thread.interrupted(), jeromq break with thread interruption
            interrupted = Thread.interrupted();
            s.close();
        } finally {
            canInterrupt = false;
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public void stopRunning() {
        if (isRunning()) {
            logger.trace("Stop handling messages");
            running = false;
            writeNotification();
            stopFunction.run();
        }
        logger.trace("Done stop handling messages");
    }

    public void interrupt(Thread holder, Runnable realInterrupt) {
        logger.trace("trying to interrupt {}", holder);
        if (canInterrupt) {
            realInterrupt.run();
        } else {
            try {
                stopRunning();
            } catch (RuntimeException ex) {
                logger.error("Failed to handle interrupt: {}", Helpers.resolveThrowableException(ex), ex);
            }
        }
    }

    /**
     * Can only be called from the same thread that consume the socket
     * @return the socket
     */
    public Socket getSocket() {
        return socket;
    }

    public Certificate getCertificate() {
        return zfactory.getKeyEntry().getCertificate();
    }

}
