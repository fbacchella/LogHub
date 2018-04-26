package loghub.zmq;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import loghub.ThreadBuilder;
import loghub.zmq.ZMQHelper.Method;
import zmq.socket.Sockets;

public class SmartContext {

    private static final Logger logger = LogManager.getLogger();

    private static SmartContext instance = null;
    public static int numSocket = 1;
    private final ZContext context = new ZContext(numSocket);
    private volatile boolean running = true;

    public static synchronized SmartContext getContext() {
        if (instance == null) {
            instance = new SmartContext();
            logger.debug("New SmartContext instance");
            ThreadBuilder.get()
            .setDaemon(true)
            .setName("terminator")
            .setRunnable(() -> {
                synchronized (SmartContext.class) {
                    if (instance != null) {
                        logger.debug("starting shutdown hook for ZMQ");
                        instance.terminate();
                    }
                }
            }).setShutdownHook(true).build();
        }
        return instance;
    }

    public boolean isRunning() {
        return running;
    }

    public Socket newSocket(Method method, Sockets type, String endpoint, int hwm, int timeout) {
        Socket socket = context.createSocket(type.ordinal());
        socket.setRcvHWM(hwm);
        socket.setSndHWM(hwm);
        socket.setSendTimeOut(timeout);
        socket.setReceiveTimeOut(timeout);;
        method.act(socket, endpoint);
        String url = endpoint + ":" + type.toString() + ":" + method.getSymbol();
        socket.setIdentity(url.getBytes());
        logger.debug("new socket: {}", url);
        return socket;
    }

    public Socket newSocket(Method method, Sockets type, String endpoint) {
        // All socket have high hwm and are blocking
        return newSocket(method, type, endpoint, 1, -1);
    }

    public void close(Socket socket) {
        try {
            logger.debug("close socket {}: {}", socket, socket);
            socket.setLinger(0);
            context.destroySocket(socket);
        } catch (ZMQException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            ZMQHelper.logZMQException(logger, "close " + socket, e);
        } catch (java.nio.channels.ClosedSelectorException e) {
            logger.debug("in close: " + e);
        } catch (Exception e) {
            logger.error("in close: " + e);
        } finally {
        }
    }

    public Future<Boolean> terminate() {
        synchronized (SmartContext.class) {
            if (running) {
                running = false;
                FutureTask<Boolean> terminator = new FutureTask<>(() -> {
                    synchronized (SmartContext.class) {
                        try {
                            logger.debug("Terminating ZMQ context");
                            instance.context.close();
                        } catch (ZMQException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                            ZMQHelper.logZMQException(logger, "terminate", e);
                        } catch (final java.nio.channels.ClosedSelectorException e) {
                            logger.error("closed selector:" + e.getMessage());
                        } catch (final Exception e) {
                            logger.error("Unexpected error:" + e.getMessage());
                            return false;
                        }
                        logger.trace("ZMQ context terminated");
                        SmartContext.instance = null;
                        return true;
                    }
                });
                ThreadBuilder.get(Boolean.class).setName("ZMQContextTerminator").setCallable(terminator).setDaemon(false).build(true);
                // Now we've send termination signals, let other threads
                // some time to finish
                Thread.yield();
                return terminator;
            } else {
                CompletableFuture<Boolean> done = new CompletableFuture<>();
                done.complete(true);
                return done;
            }
        }
    }

    public byte[] recv(Socket socket) {
        try {
            return socket.recv();
        } catch (java.nio.channels.ClosedSelectorException e ) {
            throw e;
        } catch (ZMQException | zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e ) {
            ZMQHelper.logZMQException(logger, "recv", e);
            close(socket);
            throw e;
        } catch (Exception e ) {
            logger.error("in recv: {}", e.getMessage());
            logger.error(e);
            throw e;
        }
    }

    public ZPoller getZPoller() {
        return new ZPoller(context);
    }

    public Iterable<byte[]> read(Socket receiver) throws IOException {

        @SuppressWarnings("resource")
        ZPoller zpoller = new ZPoller(context);
        zpoller.register(receiver, ZPoller.POLLIN | ZPoller.POLLERR);

        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            if (!SmartContext.this.running) {
                                return false;
                            }
                            logger.trace("waiting for next");
                            zpoller.poll(-1L);
                            if (zpoller.isError(receiver) || Thread.interrupted()) {
                                logger.trace("received kill");
                                zpoller.destroy();
                                return false;
                            } else if (! SmartContext.this.running){
                                zpoller.destroy();
                                return false;
                            } else {
                                return true;
                            }
                        } catch (RuntimeException e) {
                            ZMQHelper.logZMQException(logger, "recv", e);
                            zpoller.destroy();
                            return false;
                        }
                    }
                    @Override
                    public byte[] next() {
                        return receiver.recv();
                    }
                    @Override
                    public void remove() {
                        throw new java.lang.UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Socket[] getPair(String name) {
        String endPoint = "inproc://pair/" + name;
        Socket socket1 = newSocket(Method.BIND, Sockets.PAIR, endPoint);
        socket1.setLinger(0);
        socket1.setHWM(1);

        Socket socket2 = newSocket(Method.CONNECT, Sockets.PAIR, endPoint);
        socket2.setLinger(0);
        socket2.setHWM(1);

        return new Socket[] {socket1, socket2};
    }
}
