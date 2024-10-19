package loghub.zmq;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import lombok.Getter;
import zmq.ZError;

@Getter
public class ZMQCheckedException extends Exception {

    private final ZMQ.Error error;

    public static void checkOption(boolean test, Socket s) throws ZMQCheckedException {
        if (! test) {
            throw new ZMQCheckedException(s.errno());
        }
    }

    public static int checkCommand(int status, Socket s) throws ZMQCheckedException {
        if (status < 0 ) {
            throw new ZMQCheckedException(s.errno());
        } else {
            return status;
        }
    }

    public ZMQCheckedException(UncheckedZMQException e) {
        super(filterCause(e));
        if (e instanceof ZError.IOException) {
            IOException cause = (java.io.IOException) e.getCause();
            error = ZMQ.Error.findByCode(ZError.exccode(cause));
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

    private static Exception filterCause(UncheckedZMQException e) {
        if (e instanceof ZError.IOException) {
            return (java.io.IOException) e.getCause();
        } else {
            return e;
        }
    }

    public ZMQCheckedException(int error) {
        this.error = ZMQ.Error.findByCode(error);
    }

    public ZMQCheckedException(ZMQ.Error error) {
        this.error = error;
    }

    public boolean isInterruption() {
        return error == ZMQ.Error.EINTR || error == ZMQ.Error.ETERM;
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
