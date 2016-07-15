package loghub.zmq;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import zmq.ZError;

public class ZMQHelper {

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

    public static void logZMQException(Logger l, String prefix, RuntimeException e) {
        final ERRNO errno;
        Supplier<String> message;
        if (e instanceof ZMQException.IOException || e instanceof ZError.IOException) {
            IOException cause = (java.io.IOException) e.getCause();
            errno = ERRNO.get(exccode(cause));
            message = () -> errno.toString(prefix, e, e);
        } else if (e instanceof ZError.CtxTerminatedException) {
            errno = ERRNO.ETERM;
            message = () -> errno.toString(prefix, e, new RuntimeException("Context terminated"));
        } else if (e instanceof ZError.InstantiationException) {
            errno = ERRNO.EOTHER;
            message =  () -> errno.toString(prefix, e, e.getCause());
        } else if (e instanceof ZMQException) {
            errno = ERRNO.get(((ZMQException)e).getErrorCode());
            message = () -> errno.toString(prefix, e, e);
        } else {
            throw e;
        }
        switch(errno) {
        case EINTR: l.debug(message); break;
        case ETERM: l.debug(message); break;
        case EOTHER: l.fatal(message); break;
        default: l.error(message); break;
        }
    }

    private static int exccode(java.io.IOException e) {
        if (e instanceof SocketException) {
            return ZError.ESOCKET;
        } else if (e instanceof ClosedByInterruptException) {
            return ZError.EINTR;
        } else if (e instanceof ClosedChannelException) {
            return ZError.ENOTCONN;
        } else {
            return ZError.EIOEXC;
        }
    }

}
