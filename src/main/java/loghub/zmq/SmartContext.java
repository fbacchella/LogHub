package loghub.zmq;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import loghub.Helpers;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQHelper.Type;

import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

public class SmartContext {

    private static final Logger logger = LogManager.getLogger();

    private final static String TERMINATOR_RENDEZVOUS = "inproc://termination";

    private static SmartContext instance = null;

    public static int numSocket = 1;

    private final Map<Socket, String> sockets = new ConcurrentHashMap<>();
    private final Context context = ZMQ.context(numSocket);
    // Socket for worker control
    private final Socket controller = context.socket(ZMQ.PUB);
    private volatile boolean running = true;

    private SmartContext() {
        controller.bind(TERMINATOR_RENDEZVOUS);
    }

    public static synchronized SmartContext getContext() {
        if (instance == null || !instance.running) {
            instance = new SmartContext();
            Thread terminator = Helpers.makeSimpleThread(() -> {
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

    public Socket newSocket(Method method, Type type, String endpoint, long hwm, int timeout) {
        Socket socket = context.socket(type.type);
        socket.setRcvHWM(hwm);
        socket.setSndHWM(hwm);
        socket.setSendTimeOut(timeout);
        socket.setReceiveTimeOut(timeout);;
        method.act(socket, endpoint);
        String url = endpoint + ":" + type.toString() + ":" + method.getSymbol();
        socket.setIdentity(url.getBytes());
        sockets.put(socket, url);
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
                logger.debug("close socket {}: {}", socket, sockets.get(socket));
                if (sockets.get(socket) == null) {
                    logger.catching(Level.DEBUG, new RuntimeException());
                }
                socket.setLinger(0);
                socket.close();
            } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                ZMQHelper.logZMQException(logger, "close " + sockets.get(socket), e);
            } catch (java.nio.channels.ClosedSelectorException e) {
                logger.debug("in close: " + e);
            } catch (Exception e) {
                logger.error("in close: " + e);
            } finally {
                sockets.remove(socket);
            }
        }
    }

    public void terminate() {
        synchronized (SmartContext.class) {
            if (!running) {
                return;
            }
            running = false;
            controller.send("KILL", 0);
            Helpers.makeSimpleThread(() -> {
                try {
                    logger.trace("will terminate");
                    instance.context.term();
                } catch (ZMQException | ZMQException.IOException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "terminate", e);
                } catch (java.nio.channels.ClosedSelectorException e) {
                    logger.error("closed selector:" + e.getMessage());
                } catch (Exception e) {
                    logger.error("Unexpected error:" + e.getMessage());
                }
                logger.trace("done terminate");
            }).setName("ZMQContextTerminator").setDaemon(false).start();
            // Now we've send termination signals, let other threads
            // some time to finish
            Thread.yield();
            logger.debug("sockets to close: {}", instance.sockets);
            instance.controller.setLinger(0);
            instance.controller.close();
            for (Socket s: new ArrayList<Socket>(instance.sockets.keySet())) {
                try {
                    synchronized (s) {
                        // If someone close the socket meanwhile
                        if (!instance.sockets.containsKey(s)) {
                            continue;
                        }
                        logger.debug("forgotten socket: {}", () -> instance.sockets.get(s));
                        instance.close(s);
                    }
                } catch (ZMQException | ZMQException.IOException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException | zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "close " + instance.sockets.get(s), e);
                } catch (java.nio.channels.ClosedSelectorException e) {
                    logger.error("in close: " + e);
                } catch (Exception e) {
                    logger.error("in close: " + e);
                } finally {
                }
            }
        }
    }

    public String getURL(Socket socket) {
        return sockets.get(socket);
    }


    public Collection<String> getSocketsList() {
        return new ArrayList<>(sockets.values());
    }

    public byte[] recv(Socket socket) {
        try {
            return socket.recv();
        } catch (java.nio.channels.ClosedSelectorException e ) {
            throw e;
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e ) {
            ZMQHelper.logZMQException(logger, "recv", e);
            close(socket);
            throw e;
        } catch (Exception e ) {
            logger.error("in recv: {}", e.getMessage());
            logger.error(e);
            //close(socket);
            throw e;
        }
    }

    public Iterable<byte[]> read(final Socket receiver) throws IOException {
        final ZMQ.Socket controller = context.socket(ZMQ.SUB);
        controller.connect(TERMINATOR_RENDEZVOUS);
        controller.subscribe(new byte[] {});

        final Selector selector =  Selector.open();
        @SuppressWarnings("resource")
        final ZPoller zpoller = new ZPoller(selector);
        zpoller.register(receiver, ZPoller.POLLIN | ZPoller.POLLERR);
        zpoller.register(controller, ZPoller.POLLIN | ZPoller.POLLERR);

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
                            if (zpoller.isReadable(controller) || zpoller.isError(controller) || zpoller.isError(receiver) || Thread.interrupted()) {
                                logger.trace("received kill");
                                controller.close();
                                zpoller.destroy();
                                try {
                                    selector.close();
                                } catch (IOException e) {
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
