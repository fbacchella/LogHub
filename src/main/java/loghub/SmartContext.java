package loghub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import zmq.ZMQHelper;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.SocketInfo;
import zmq.ZMQHelper.Type;

import org.zeromq.ZMQException;

public class SmartContext {

    private static final Logger logger = LogManager.getLogger();

    private final static String TERMINATOR_RENDEZVOUS = "inproc://termination";

    private static SmartContext instance = null;

    private final Map<Socket, String> sockets = new ConcurrentHashMap<>();
    public static int numSocket = 1;
    private final Context context = ZMQ.context(numSocket);
    private final Socket controller;
    private final List<Thread> proxies = new ArrayList<>();

    private SmartContext() {
        // Socket for worker control
        controller = context.socket(ZMQ.PUB);
        controller.bind(TERMINATOR_RENDEZVOUS);       
    }

    public static synchronized SmartContext getContext() {
        if(instance == null) {
            instance = new SmartContext();
        }
        return instance;
    }

    public static synchronized Thread terminate() {
        if(instance != null) {
            Thread t = instance._terminate();
            instance = null;
            return t;
        } else {
            return null;
        }
    }

    private Thread _terminate() {
        controller.send("KILL", 0);
        // Now we've send termination signals, let other threads
        // some time to finish
        Thread.yield();
        logger.debug("proxies to kill: {}", proxies);
        logger.debug("sockets to close: {}", sockets);
        controller.setLinger(0);
        controller.close();
        for(Socket s: sockets.keySet()) {
            try {
                logger.debug("forgotten socket: {}", () -> sockets.get(s));
                s.setLinger(0);
                s.close();
            } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                ZMQHelper.logZMQException("close " + sockets.get(s), e);
            } catch (java.nio.channels.ClosedSelectorException e) {
                logger.error("in close: " + e);
            } catch (Exception e) {
                logger.error("in close: " + e);
            } finally {
            }
        }
        final Thread terminator = new Thread() {
            @Override
            public void run() {
                try {
                    logger.trace("will terminate");
                    context.term();
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException("terminate", e);
                } catch (java.nio.channels.ClosedSelectorException e) {
                    logger.error("closed selector:" + e.getMessage());
                } catch (Exception e) {
                    logger.error("Unexpected error:" + e.getMessage());
                }
                logger.trace("done terminate");
            }
        };
        terminator.setName("terminator");
        terminator.start();
        Thread.yield();
        return terminator;
    }

    public Socket newSocket(Method method, Type type, String endpoint) {
        Socket socket = context.socket(type.type);
        method.act(socket, endpoint);
        String url = endpoint + ":" + type.toString() + ":" + method.getSymbol();
        sockets.put(socket, url);
        logger.debug("new socket: {}", url);
        return socket;
    }

    public void close(Socket socket) {
        try {
            logger.debug("close socket: " + sockets.get(socket));
            socket.setLinger(0);
            socket.close();
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            ZMQHelper.logZMQException("close " + sockets.get(socket), e);
        } catch (java.nio.channels.ClosedSelectorException e) {
            logger.debug("in close: " + e);
        } catch (Exception e) {
            logger.error("in close: " + e);
        } finally {
            sockets.remove(socket);
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
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e ) {
            ZMQHelper.logZMQException("recv", e);
            close(socket);
            throw e;
        } catch (Exception e ) {
            logger.error("in recv: {}", e.getMessage());
            close(socket);
            throw e;
        }
    }

    public Iterable<byte[]> read(final Socket receiver) {
        final ZMQ.Socket controller = context.socket(ZMQ.SUB);
        controller.connect(TERMINATOR_RENDEZVOUS);
        controller.subscribe("".getBytes());

        final ZMQ.Poller items = new ZMQ.Poller(2);
        items.register(receiver, ZMQ.Poller.POLLIN);
        items.register(controller, ZMQ.Poller.POLLIN);
        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {
                    @Override
                    public boolean hasNext() {
                        logger.trace("waiting for next");
                        try {
                            items.poll();
                        } catch (Exception e) {
                            controller.close();
                            throw e;
                        }
                        if (items.pollin(1) || Thread.interrupted()) {
                            logger.trace("received kill");
                            controller.close();
                            return false;
                        } else {
                            return true;
                        }
                    }
                    @Override
                    public byte[] next() {
                        return recv(receiver);
                    }
                    @Override
                    public void remove() {
                        throw new java.lang.UnsupportedOperationException();
                    }                    
                };
            }            
        };
    }

    public void proxy(String name, SocketInfo socketIn, SocketInfo socketOut) {
        Socket in = newSocket(socketIn.method, socketIn.type, socketIn.endpoint);
        Socket out = newSocket(socketOut.method, socketOut.type, socketOut.endpoint);
        proxy(name, in, out);
    }

    public void proxy(final String name, final Socket socketIn, final Socket socketOut) {
        logger.debug("new proxy from {} to {}", () -> getURL(socketIn), () -> getURL(socketOut));
        new Thread() {
            {
                setDaemon(true);
                setName("proxy-" + name);
                start();
                proxies.add(this);
            }
            @Override
            public void run() {
                try {
                    for(byte[] msg: read(socketIn)) {
                        socketOut.send(msg);
                    }
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException("proxy", e);
                } catch (Exception e) {
                    logger.error("in proxy: {}", e);
                } finally {
                    logger.debug("Stopped proxy {}", getName());
                    close(socketIn);
                    close(socketOut);                    
                }
            }
        };
    }

}
