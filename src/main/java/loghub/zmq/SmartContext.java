package loghub.zmq;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import loghub.Helpers;
import loghub.Helpers.SimplifiedThread;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQHelper.Type;

public class SmartContext {

    private static final Logger logger = LogManager.getLogger();

    private static SmartContext instance = null;
    public static int numSocket = 1;
    private final ZContext context = new ZContext(numSocket);
    private volatile boolean running = true;

    public static synchronized SmartContext getContext() {
        if (instance == null || !instance.running) {
            instance = new SmartContext();
            Thread terminator = new Helpers.SimplifiedThreadRunnable(() -> {
                synchronized (SmartContext.class) {
                    if (instance != null) {
                        logger.debug("starting shutdown hook for ZMQ");
                        instance.terminate();
                        instance = null;
                    }
                }
            }).setDaemon(false)
                    .setName("terminator")
                    .thread;
            Runtime.getRuntime().addShutdownHook(terminator) ;
        }
        return instance;
    }

    public boolean isRunning() {
        return running;
    }

    public Socket newSocket(Method method, Type type, String endpoint, int hwm, int timeout) {
        Socket socket = context.createSocket(type.type);
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

    public Socket newSocket(Method method, Type type, String endpoint) {
        // All socket have high hwm and are blocking
        return newSocket(method, type, endpoint, 1, -1);
    }

    public void close(Socket socket) {
        synchronized(socket){
            try {
                logger.debug("close socket {}: {}", socket, socket);
                socket.setLinger(0);
                socket.close();
            } catch (ZMQException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                ZMQHelper.logZMQException(logger, "close " + socket, e);
            } catch (java.nio.channels.ClosedSelectorException e) {
                logger.debug("in close: " + e);
            } catch (Exception e) {
                logger.error("in close: " + e);
            } finally {
            }
        }
    }

    public Future<Boolean> terminate() {
        synchronized (SmartContext.class) {
            if (!running) {
                return new FutureTask<Boolean>(() -> true);
            }
            running = false;
            SimplifiedThread<Boolean> terminator = new Helpers.SimplifiedThread<Boolean>(() -> {
                try {
                    logger.trace("will terminate");
                    instance.context.close();
                } catch (ZMQException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "terminate", e);
                } catch (final java.nio.channels.ClosedSelectorException e) {
                    logger.error("closed selector:" + e.getMessage());
                } catch (final Exception e) {
                    logger.error("Unexpected error:" + e.getMessage());
                    return false;
                }
                logger.trace("done terminate");
                return true;
            }).setName("ZMQContextTerminator").setDaemon(false).start();
            // Now we've send termination signals, let other threads
            // some time to finish
            Thread.yield();
            return terminator.task;
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

    public Iterable<byte[]> read(Socket receiver) throws IOException {

        final Selector selector =  Selector.open();
        @SuppressWarnings("resource")
        final ZPoller zpoller = new ZPoller(selector);
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
                                try {
                                    selector.close();
                                } catch (IOException e) {
                                    logger.debug("Failed close because of {}", () -> e.getMessage());
                                    logger.catching(Level.DEBUG, e);
                                };
                                return false;
                            } else {
                                return true && SmartContext.this.running;
                            }
                        } catch (RuntimeException e) {
                            ZMQHelper.logZMQException(logger, "recv", e);
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

}
