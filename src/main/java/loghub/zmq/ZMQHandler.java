package loghub.zmq;

import java.io.IOException;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.cert.Certificate;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ.Event;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.AbstractBuilder;
import loghub.Helpers;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQSocketFactory.SocketBuilder;
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
        String security = Mechanisms.NULL.toString();
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
        BiConsumer<Socket, Event> eventCallback = null;


        public Builder<M> setServerPublicKeyToken(String serverKeyToken) {
            this.serverPublicKey = serverKeyToken != null ? ZMQHelper.parseServerIdentity(serverKeyToken) : null;
            return this;
        }

        @Override
        public ZMQHandler<M> build() {
            return new ZMQHandler<M>(this);
        }
    }

    private final Logger logger;
    private final String socketUrl;
    private final int mask;
    private final Function<Socket, M> receive;
    private final BiFunction<Socket, M, Boolean> send;
    private final String pairId;
    // The stopFunction can be used to do a little cleaning and verifications before the join
    private final Runnable stopFunction;

    private volatile boolean running = false;
    private PrepareFactory makeThreadLocal;
    private Socket socket;
    private Socket socketEndPair;
    private Socket socketMonitor;
    private Thread runningThread = null;
    private ZPoller pooler;
    // Settings of the factory can be delayed
    @Setter
    private ZMQSocketFactory zfactory = null;
    //Interrupt is only allowed outside of ZMQ poll or socket options, this flag protect that
    private volatile boolean canInterrupt = true;
    private final BiConsumer<Socket, Event> eventCallback;

    private ZMQHandler(Builder<M> builder) {
        this.logger = builder.logger;
        this.socketUrl = builder.socketUrl;
        this.mask = builder.mask;
        this.pairId = builder.name + "/" + UUID.randomUUID();
        this.stopFunction = builder.stopFunction;
        this.zfactory = builder.zfactory;
        this.send = builder.send;
        this.receive = builder.receive;
        this.eventCallback = builder.eventCallback;
        // Socket creation is delayed
        // So the socket is created in the using thread
        makeThreadLocal = () -> {
            makeThreadLocal = null;
            runningThread = Thread.currentThread();
            SocketBuilder sbuilder = zfactory.getBuilder(builder.method, builder.type, socketUrl).setTopic(builder.topic).setImmediate(false);
            if (eventCallback != null) {
                socketMonitor = zfactory.getBuilder(Method.CONNECT, SocketType.PAIR, "inproc://monitor/" + pairId).build();
                sbuilder.setMonitor("inproc://monitor/" + pairId);
            } else {
                socketMonitor = null;
            }
            Mechanisms security = Optional.ofNullable(builder.security)
                                          .map(s -> s.toUpperCase(Locale.ENGLISH))
                                          .map(Mechanisms::valueOf)
                                          .orElse(Mechanisms.NULL);
            switch (security) {
            case CURVE: 
                logger.trace("Activating Curve security on a socket");
                sbuilder.setCurveKeys(Optional.ofNullable(builder.keyEntry).orElse(zfactory.getKeyEntry()));
                break;
            case NULL:
                break;
            default:
                throw new IllegalArgumentException("Security  "+ builder.security + "not handled");
            }
            socket = sbuilder.build();
            pooler = zfactory.getZPoller();
            socketEndPair = zfactory.getBuilder(Method.BIND, SocketType.PAIR, "inproc://stop/" + pairId).build();
            logger.trace("Socket end pair will be {}", socketEndPair);
            pooler.register(socket, mask | ZPoller.ERR);
            pooler.register(socketEndPair, ZPoller.IN | ZPoller.ERR);
            if (eventCallback != null) {
                pooler.register(socketMonitor, ZPoller.IN | ZPoller.ERR);
            }
            running = true;
            if (builder.latch != null) {
                builder.latch.countDown();
            }
        };
    }

    public void start() throws ZMQCheckedException {
        makeThreadLocal.run();
    }

    public M dispatch(M message) throws ZMQCheckedException {
        assert Thread.currentThread() == runningThread;
        logger.trace("One dispatch loop");
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
                    int pEvents = ZMQCheckedException.checkCommand(socketEndPair.getEvents(),
                                                                   socketEndPair);
                    int mEvents;
                    if (socketMonitor != null) {
                        mEvents = ZMQCheckedException.checkCommand(socketMonitor.getEvents(),
                                                                   socketMonitor);
                    } else {
                        mEvents = 0;
                    }
                    // Received an error, end processing
                    if ((sEvents & ZPoller.ERR) != 0) {
                        logEvent("Error on socket", socket, sEvents);
                        throw new ZMQCheckedException(socket.errno());
                    } else if ((pEvents & ZPoller.ERR) != 0) {
                        logEvent("Error on socket", socketEndPair, pEvents);
                        throw new ZMQCheckedException(socketEndPair.errno());
                    } else if ((mEvents & ZPoller.ERR) != 0) {
                        logEvent("Error on socket", socketMonitor, mEvents);
                        throw new ZMQCheckedException(socketEndPair.errno());
                    }
                    // A monitor or end signal event
                    if ((mEvents & ZPoller.IN) != 0) {
                        logEvent("Monitor signal", socketMonitor, mEvents);
                        Event ev = Event.recv(socketMonitor);
                        eventCallback.accept(socket, ev);
                    }
                    if ((pEvents & ZPoller.IN) != 0) {
                        logEvent("Stop signal", socketEndPair, pEvents);
                        running = false;
                        socketEndPair.recv();
                        break;
                    }
                    // A event on the main socket, end the loop
                    if ((sEvents & ZPoller.OUT) != 0) {
                        logEvent("Message signal", socket, sEvents);
                        if (! send.apply(socket, message)) {
                            throw new ZMQCheckedException(socket.errno());
                        }
                        return null;
                    }
                    if ((sEvents & ZPoller.IN) != 0) {
                        logEvent("Message signal", socket, sEvents);
                        M received = this.receive.apply(socket);
                        if (received == null) {
                            throw new ZMQCheckedException(socket.errno());
                        } else {
                            return received;
                        }
                    }
               }
            } catch (UncheckedZMQException ex) {
                throw new ZMQCheckedException(ex);
            } finally {
                canInterrupt = true;
            }
        } 
        return null;
    }

    public void logEvent(String prefix, Socket socket, int event) {
        logger.trace("{}: receiving {}{}{} on {}",
                     () -> prefix,
                     () -> (event & ZPoller.ERR) != 0 ? "E" : ".",
                     () -> (event & ZPoller.OUT) != 0 ? "O" : ".",
                     () -> (event & ZPoller.IN) != 0  ? "I" : ".",
                     () -> socket);
    }

    public boolean isRunning() {
        return running && ! runningThread.isInterrupted();
    }

    @Override
    public void close() {
        assert Thread.currentThread() == runningThread;
        assert ! running;
        Stream.of(socket, socketEndPair, socketMonitor)
        .filter(Objects::nonNull)
        .forEach(this::close);
   }

    public void close(Socket s) {
        boolean interrupted = false;
        try {
            canInterrupt = false;
            // Keep Thread.interrupted(), jeromq break with thread interruption
            interrupted = Thread.interrupted();
            s.close();
       } catch (UncheckedZMQException e) {
            e.printStackTrace();
        } finally {
            canInterrupt = false;
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public void stopRunning() throws IOException, ZMQCheckedException {
        if (isRunning()) {
            logger.trace("Stop handling messages");
            running = false;
            try (Socket stopStartPair = zfactory.getBuilder(Method.CONNECT, SocketType.PAIR, "inproc://stop/" + pairId).build()) {
                stopStartPair.send(new byte[] {});
                logger.debug("Listening stopped");
            } catch (UncheckedZMQException ex) {
                throw new ZMQCheckedException(ex);
            }
            stopFunction.run();
        }
        logger.trace("Done stop handling messages");
    }

    public void interrupt(Thread holder, Runnable realInterrupt) {
        logger.trace("trying to interrupt");
        if (canInterrupt) {
            realInterrupt.run();
        } else {
            try {
                stopRunning();
            } catch (IOException | ZMQCheckedException ex) {
                logger.error("Failed to handle interrupt: {}", Helpers.resolveThrowableException(ex), ex);
            }
        }
    }

    /**
     * Can only be called from the the same thread that consume the socket
     * @return
     * @throws ZMQCheckedException if socket creation failed
     */
    public Socket getSocket() throws ZMQCheckedException {
        return socket;
    }

    public Certificate getCertificate() {
        return zfactory.getKeyEntry().getCertificate();
    }

}
