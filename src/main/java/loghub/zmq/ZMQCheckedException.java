package loghub.zmq;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import zmq.ZError;

public class ZMQCheckedException extends Exception {

    private final ZMQ.Error error;

    public static void checkOption(boolean test, Socket s) throws ZMQCheckedException {
        if (! test) {
            throw new ZMQCheckedException(s.errno());
        }
    }

    public ZMQCheckedException(UncheckedZMQException e) {
        super(e);
        if (e instanceof ZError.IOException) {
            IOException cause = (java.io.IOException) e.getCause();
            error = ZMQ.Error.findByCode(exccode(cause));
        } else if (e instanceof ZError.CtxTerminatedException) {
            error = ZMQ.Error.ETERM;
        } else if (e instanceof ZError.InstantiationException) {
            throw e;
        } else if (e instanceof ZMQException) {
            error = ZMQ.Error.findByCode(((ZMQException)e).getErrorCode());
        } else {
            throw new IllegalStateException("Unhandled ZMQ Exception", e);
        }
        setStackTrace(e.getStackTrace());
    }

    public ZMQCheckedException(int error) {
        this.error = ZMQ.Error.findByCode(error);
    }

    public ZMQCheckedException(ZMQ.Error error) {
        this.error = error;
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

    public ZMQ.Error getError() {
        return error;
    }

    @Override
    public String getMessage() {
        return error.getMessage();
    }

    public static void logZMQException(Logger l, String prefix, UncheckedZMQException e) {
        ZMQCheckedException zex = new ZMQCheckedException(e);
        switch (zex.error) {
        case EINTR:
        case ETERM:
            l.debug(prefix, zex.getMessage());
            break;
        default:
            l.error(prefix, zex.getMessage());
        }
        l.catching(Level.DEBUG, e);
    }

}
