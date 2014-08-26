package loghub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class ZMQManager {
    
    private static final Logger logger = LogManager.getLogger();

    public static class SocketInfo {
        final Method method;
        final Type type;
        final String endpoint;
        public SocketInfo(Method method, Type type, String endpoint) {
            super();
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
    };

    public enum Method {
        CONNECT {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.connect(address); }
        },
        BIND {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.bind(address); }
        };
        abstract void act(ZMQ.Socket socket, String address);
    }

    public enum Type {
        PUSH(ZMQ.PUSH),
        PULL(ZMQ.PULL),
        PUB(ZMQ.PUB),
        DEALER(ZMQ.DEALER),
        ROUTER(ZMQ.ROUTER);
        public final int type;
        Type(int type) {
            this.type = type;
        }
    }

    private final static Set<Socket> sockets = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
    private static final int numSocket = 1;
    private static Context context = ZMQ.context(numSocket);
    private static List<Thread> proxies = new ArrayList<>();
    private ZMQManager() {
    }

    public static Context getContext() {
        return context;
    }

    public static void terminate() {
        final Thread terminator = new Thread() {
            {
                start();
            }

            @Override
            public void run() {
                try {
                    context.term();
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                    logZMQException("term", e);
                } catch (java.nio.channels.ClosedSelectorException e) {
                    System.out.println("in term: " + e);
                } catch (Exception e) {
                    System.out.println("in term: " + e);
                }
            }
        };
        try {
            terminator.join(2000);
        } catch (InterruptedException e) {
        }
        for(Thread t: proxies) {
            t.interrupt();
        }
        proxies.retainAll(Collections.emptyList());
        if(sockets.size() > 0) {
            for(Socket s: sockets) {
                logger.error("Unclosed socket: {}", new String(s.getIdentity()));
            }
            throw new RuntimeException("Some sockets still open");
        }
        context = ZMQ.context(numSocket);
    }

    public static Socket newSocket(Method method, Type type, String endpoint) {
        Socket socket = context.socket(type.type);
        method.act(socket, endpoint);
        socket.setIdentity((type.toString() + ":" + method.toString() + ":" + endpoint).getBytes());
        sockets.add(socket);
        return socket;
    }

    public static void logZMQException(String prefix, Exception e0) {
        try {
            e0.getStackTrace()[0].getMethodName();
            throw e0;
        } catch (zmq.ZError.CtxTerminatedException e) {
            logger.debug("ZMQ context terminated in {}", prefix);
        } catch (ZMQException e) {
            logger.debug("in {}: ZMQException: ", prefix, e);
        } catch (ZMQException.IOException e) {
            logger.debug("in {}: ZMQException.IOException: {}", prefix, e.getCause());
        } catch (zmq.ZError.IOException e) {
            logger.debug("in {}: zmq.ZError.IOException: {}", prefix, e.getCause());
        } catch (zmq.ZError.InstantiationException e) {
            logger.error("in {}: zmq.ZError.InstantiationException: {}", prefix, e);
        } catch (Exception e) {
            logger.debug("bad previous catch: {}", e);
        }
    }

    public static void close(Socket socket) {
        try {
            socket.setLinger(0);
            socket.close();
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            logZMQException("close", e);
        } catch (java.nio.channels.ClosedSelectorException e) {
            System.out.println("in close: " + e);
        } catch (Exception e) {
            System.out.println("in close: " + e);
        } finally {
            sockets.remove(socket);
        }
    }

    public static void proxy(final String name, final SocketInfo socketIn, final SocketInfo socketOut) {
        new Thread() {
            private final Socket in;
            private final Socket out;
            {
                setDaemon(true);
                setName(name);
                in = newSocket(socketIn.method, socketIn.type, socketIn.endpoint);
                out = newSocket(socketOut.method, socketOut.type, socketOut.endpoint);
                start();
                proxies.add(this);
            }
            @Override
            public void run() {
                System.out.println("Starting proxy " + getName());
                try {
                    ZMQ.proxy(in, out, null);
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                    logZMQException("proxy", e);
                } catch (Exception e) {
                    System.out.println("in proxy: " + e);
                }
                close(in);
                close(out);
                System.out.println("Stopping proxy " + getName());
            }
        };
    }

    public static void proxy(final String name, Socket socketIn, Socket socketOut) {
        try {
            ZMQ.proxy(socketIn, socketOut, null);
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            logZMQException("proxy", e);
        } catch (Exception e) {
            System.out.println("in proxy: " + e);
        }

        close(socketIn);
        close(socketOut);
    }

    public static byte[] recv(Socket socket) {
        try {
            return socket.recv();
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e ) {
            logZMQException("recv", e);
            close(socket);
            throw e;
        } catch (Exception e ) {
            System.out.println(e);
            close(socket);
            throw e;
        }
    }
}
