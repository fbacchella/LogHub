package loghub.zmq;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.Errors;
import org.zeromq.UncheckedZMQException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import lombok.Getter;
import zmq.ZError;

@Getter
public class ZMQCheckedException extends Exception {

    private final Errors error;

    public static void checkOption(boolean test, Socket s) throws ZMQCheckedException {
        if (! test) {
            throw new ZMQCheckedException(s.errno());
        }
    }

    public static int checkCommand(int status, Socket s) throws ZMQCheckedException {
        if (status < 0) {
            throw new ZMQCheckedException(s.errno());
        } else {
            return status;
        }
    }

    public ZMQCheckedException(UncheckedZMQException e) {
        super(filterCause(e));
        switch (e) {
        case ZError.IOException ignored -> {
            IOException cause = (IOException) e.getCause();
            error = Errors.findByCode(ZError.exccode(cause));
        }
        case ZError.CtxTerminatedException ignored -> error = Errors.ETERM;
        case ZError.InstantiationException ignored -> throw e;
        case ZMQException ex -> error = Errors.findByCode(ex.getErrorCode());
        case null, default -> throw new IllegalStateException("Unhandled ZMQ Exception", e);
        }
        setStackTrace(e.getStackTrace());
    }

    private static Exception filterCause(UncheckedZMQException e) {
        if (e instanceof ZError.IOException) {
            return (IOException) e.getCause();
        } else {
            return e;
        }
    }

    public ZMQCheckedException(int error) {
        this.error = Errors.findByCode(error);
    }

    public ZMQCheckedException(Errors error) {
        this.error = error;
    }

    public boolean isInterruption() {
        return error == Errors.EINTR || error == Errors.ETERM;
    }

    @Override
    public String getMessage() {
        return error.getMessage();
    }

    public static void logZMQException(Logger l, String prefix, UncheckedZMQException e) {
        ZMQCheckedException zex = new ZMQCheckedException(e);
        switch (zex.error) {
        case EINTR, ETERM:
            l.debug(prefix, zex.getMessage());
            break;
        default:
            l.error(prefix, zex.getMessage());
        }
        l.catching(Level.DEBUG, e);
    }

}
