package loghub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import zmq.ZError;

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

    private final static Map<Integer, ERRNO> map = new HashMap<>();

    public enum ERRNO {
        EOTHER(-1),
        EINTR(ZError.EINTR),
        EACCESS(ZError.EACCESS),
        EFAULT(ZError.EFAULT),
        EINVAL(ZError.EINVAL),
        EAGAIN(ZError.EAGAIN),
        EINPROGRESS(ZError.EINPROGRESS),
        EPROTONOSUPPORT(ZError.EPROTONOSUPPORT),
        ENOTSUP(ZError.ENOTSUP),
        EADDRINUSE(ZError.EADDRINUSE),
        EADDRNOTAVAIL(ZError.EADDRNOTAVAIL),
        ENETDOWN(ZError.ENETDOWN),
        ENOBUFS(ZError.ENOBUFS),
        EISCONN(ZError.EISCONN),
        ENOTCONN(ZError.ENOTCONN),
        ECONNREFUSED(ZError.ECONNREFUSED),
        EHOSTUNREACH(ZError.EHOSTUNREACH),
        EFSM(ZError.EFSM),
        ENOCOMPATPROTO(ZError.ENOCOMPATPROTO),
        ETERM(ZError.ETERM) {
            public String toString(String context, Exception exceptionToClass, Throwable exceptionToMessage) {
                return String.format("[%s] %s: %s", context, exceptionToClass.getClass().getCanonicalName(), toStringMessage());
            }            
        },
        EMTHREAD(ZError.EMTHREAD),
        EIOEXC(ZError.EIOEXC),
        ESOCKET(ZError.ESOCKET),
        EMFILE(ZError.EMFILE);

        public final int code;
        ERRNO(int code) {
            this.code = code;
            map.put(code, this);
        }

        public static ERRNO get(int code) {
            return map.get(code);
        }
        public String toStringMessage() {
            return ZError.toString(code);
        }            
        public String toString(String context, Exception exceptionToClass, Throwable exceptionToMessage) {
            return String.format("[%s] %s %s", context, toStringMessage(), exceptionToMessage);
        }
    }

    public enum Method {
        CONNECT {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.connect(address); }

            @Override
            public char getSymbol() {
                return '(';
            }
        },
        BIND {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.bind(address); }

            @Override
            public char getSymbol() {
                return ')';
            }
        };
        abstract void act(ZMQ.Socket socket, String address);
        abstract char getSymbol();
    }

    public enum Type {
        PUSH(ZMQ.PUSH),
        PULL(ZMQ.PULL),
        PUB(ZMQ.PUB),
        SUB(ZMQ.SUB),
        DEALER(ZMQ.DEALER),
        ROUTER(ZMQ.ROUTER);
        public final int type;
        Type(int type) {
            this.type = type;
        }
    }

    private final static Map<Socket, String> sockets = new ConcurrentHashMap<>();
    private static final int numSocket = 1;
    private static Context context = ZMQ.context(numSocket);
    private static List<Thread> proxies = new ArrayList<>();
    private ZMQManager() {
        logger.debug("new context " + context);
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
                    logZMQException("terminate", e);
                } catch (java.nio.channels.ClosedSelectorException e) {
                    logger.error("closed selector:" + e.getMessage());
                } catch (Exception e) {
                    logger.error("Unexpected error:" + e.getMessage());
                }
            }
        };
        for(Thread t: proxies) {
            t.interrupt();
        }
        try {
            terminator.join(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Interrupted while terminating");
        }
        proxies.retainAll(Collections.emptyList());
        if(sockets.size() > 0) {
            for(String s: sockets.values()) {
                logger.error("Unclosed socket: {}", s);
            }
            throw new RuntimeException("Some sockets still open");
        }
        context = ZMQ.context(numSocket);
    }

    public static Socket newSocket(Method method, Type type, String endpoint) {
        Socket socket = context.socket(type.type);
        method.act(socket, endpoint);
        sockets.put(socket, endpoint + ":" + type.toString() + ":" + method.getSymbol());
        return socket;
    }

    public static void logZMQException(String prefix, RuntimeException e0) {
        ERRNO errno = ERRNO.EOTHER;
        String message;
        try {
            throw e0;
        } catch (ZMQException e) {
            errno = ERRNO.get(e.getErrorCode());
            message = errno.toString(prefix, e, e);
        } catch (zmq.ZError.CtxTerminatedException e) {
            errno = ERRNO.ETERM;
            message = errno.toString(prefix, e, new RuntimeException("Context terminated"));
        } catch (zmq.ZError.IOException e) {
            errno = ERRNO.get(zmq.Error.exccode(e));
            message = errno.toString(prefix, e, e.getCause());
        } catch (zmq.ZError.InstantiationException e) {
            message =  errno.toString(prefix, e, e.getCause());
        } catch (RuntimeException e) {
            throw e;
        }
        switch(errno) {
        case ETERM: logger.debug(message); break;
        case EOTHER: logger.fatal(message); break;
        default: logger.error(message); break;
        }
    }

    public static void close(Socket socket) {
        try {
            socket.setLinger(1);
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
                try {
                    ZMQ.proxy(in, out, null);
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
                    logZMQException("proxy", e);
                } catch (Exception e) {
                    System.out.println("in proxy: " + e);
                }
                close(in);
                close(out);
                logger.debug("Stopped proxy {}", getName());
            }
        };
    }

    public static void proxy(final String name, Socket socketIn, Socket socketOut) {
        try {
            ZMQ.proxy(socketIn, socketOut, null);
        } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.CtxTerminatedException|zmq.ZError.InstantiationException e) {
            logZMQException("proxy", e);
        } catch (Exception e) {
            logger.error("in proxy: {}", e.getMessage());
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
            logger.error("in recv: {}", e.getMessage());
            close(socket);
            throw e;
        }
    }

    public static Collection<String> getSocketsList() {
        return new ArrayList<>(sockets.values());
    }
}
