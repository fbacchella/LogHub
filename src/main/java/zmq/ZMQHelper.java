package zmq;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import loghub.SmartContext;

import org.zeromq.ZMQException;

import zmq.ZError;

public class ZMQHelper {

    private static final Logger logger = LogManager.getLogger();

    public static class SocketInfo {
        public final Method method;
        public final Type type;
        public final String endpoint;
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
            public void act(ZMQ.Socket socket, String address) { socket.connect(address); }

            @Override
            public char getSymbol() {
                return '-';
            }
        },
        BIND {
            @Override
            public void act(ZMQ.Socket socket, String address) { socket.bind(address); }

            @Override
            public char getSymbol() {
                return 'O';
            }
        };
        public abstract void act(ZMQ.Socket socket, String address);
        public abstract char getSymbol();
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

    private ZMQHelper() {
    }
    
    @Deprecated
    public static void terminate() {
        SmartContext.terminate();
    }

    @Deprecated
    public static Socket newSocket(Method method, Type type, String endpoint) {
        return SmartContext.getContext().newSocket(method, type, endpoint);
    }

    @Deprecated
    public static String getURL(Socket socket) {
        return SmartContext.getContext().getURL(socket);
    }

    @Deprecated
    public static Iterable<byte[]> read(final Socket receiver) {
        return SmartContext.getContext().read(receiver);
    }

    @Deprecated
    public static void close(Socket socket) {
        SmartContext.getContext().close(socket);
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
            errno = ERRNO.get(ZError.exccode((java.io.IOException) e.getCause()));
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

    @Deprecated
    public static void proxy(final String name, final SocketInfo socketIn, final SocketInfo socketOut) {
        SmartContext.getContext().proxy(name, socketIn, socketOut);
    }

    @Deprecated
    public static void proxy(final String name, Socket socketIn, Socket socketOut) {
        SmartContext.getContext().proxy(name, socketIn, socketOut);
    }

    @Deprecated
    public static Collection<String> getSocketsList() {
        return SmartContext.getContext().getSocketsList();
    }

    @Deprecated
    public static byte[] recv(Socket pipe) {
        return SmartContext.getContext().recv(pipe);
    }
}
