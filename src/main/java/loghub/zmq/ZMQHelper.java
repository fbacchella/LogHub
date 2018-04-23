package loghub.zmq;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import zmq.ZError;
import zmq.socket.Sockets;

public class ZMQHelper {

    public static class SocketInfo {
        public final Method method;
        public final Sockets type;
        public final String endpoint;
        public SocketInfo(Method method, Sockets type, String endpoint) {
            super();
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
    };

    private final static Map<Integer, ERRNO> map = new HashMap<>();

    public enum ERRNO {
        EOTHER(-1, Level.FATAL),
        EINTR(ZError.EINTR, Level.DEBUG),
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
        ETERM(ZError.ETERM, Level.DEBUG) {
            public String toString(String prefix, Exception exceptionToClass, Throwable exceptionToMessage) {
                return String.format("%s [%s] %s", prefix, exceptionToClass.getClass().getCanonicalName(), toStringMessage());
            }
        },
        EMTHREAD(ZError.EMTHREAD),
        EIOEXC(ZError.EIOEXC),
        ESOCKET(ZError.ESOCKET),
        EMFILE(ZError.EMFILE);

        public final int code;
        public final Level level;

        ERRNO(int code) {
            this(code, Level.ERROR);
        }

        ERRNO(int code, Level level) {
            this.code = code;
            this.level = level;
            map.put(code, this);
        }

        public static ERRNO get(int code) {
            return map.get(code);
        }
        public String toStringMessage() {
            return ZError.toString(code);
        }
        public String toString(String prefix, Exception exceptionToClass, Throwable exceptionToMessage) {
            return String.format("%s [%s] %s", prefix, toStringMessage(), exceptionToMessage);
        }

        public String toString(String prefix, RuntimeException e) {
            return String.format("%s %s", prefix, toStringMessage());
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

    private ZMQHelper() {
    }

    public static void logZMQException(Logger l, String prefix, RuntimeException e) {
        ERRNO errno;
        Supplier<String> message;
        if (e instanceof ZError.IOException) {
            IOException cause = (java.io.IOException) e.getCause();
            errno = ERRNO.get(exccode(cause));
            message = () -> errno.toString(prefix, e);
        } else if (e instanceof ZError.CtxTerminatedException) {
            errno = ERRNO.ETERM;
            message = () -> errno.toString(prefix, e, new RuntimeException("Context terminated"));
        } else if (e instanceof ZError.InstantiationException) {
            errno = ERRNO.EOTHER;
            message =  () -> errno.toString(prefix, e, e.getCause());
        } else if (e instanceof ZMQException) {
            errno = ERRNO.get(((ZMQException)e).getErrorCode());
            message = () -> errno.toString(prefix, e);
        } else {
            throw e;
        }
        l.log(errno.level, message);
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
