package loghub.receivers;

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Beans;
import loghub.configuration.Properties;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper;

@Beans({"method", "listen", "type", "hwm"})
public class ZMQ extends Receiver {

    private static final SmartContext ctx = SmartContext.getContext();

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String listen = "tcp://localhost:2120";
    private ZMQHelper.Type type = ZMQHelper.Type.SUB;
    private int hwm = 1000;
    private Socket listeningSocket;

    public ZMQ(BlockingQueue<Event> outQueue, Pipeline processors) {
        super(outQueue, processors);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            listeningSocket = ctx.newSocket(method, type, listen);
            listeningSocket.setImmediate(false);
            listeningSocket.setReceiveTimeOut(-1);
            listeningSocket.setHWM(hwm);
            if(type == ZMQHelper.Type.SUB){
                listeningSocket.subscribe(new byte[] {});
            }
        } catch (org.zeromq.ZMQException e) {
            ZMQHelper.logZMQException(logger, "failed to start ZMQ input " + listen + ":", e);
            logger.catching(Level.DEBUG, e.getCause());
            listeningSocket = null;
            return false;
        }
        return super.configure(properties);
    }

    @Override
    protected Iterator<Event> getIterator() {
        if (listeningSocket == null || !ctx.isRunning()) {
            return null;
        }
        final Iterator<byte[]> generator;
        try {
            generator = ctx.read(listeningSocket).iterator();
        } catch (IOException e) {
            logger.error("error starting to listen on {}: {}", listen, e.getMessage());
            return null;
        }
        return new Iterator<Event>() {

            @Override
            public boolean hasNext() {
                return ctx.isRunning() && generator.hasNext();
            }

            @Override
            public Event next() {
                try {
                    byte[] msg = generator.next();
                    return decode(msg);
                } catch (ClosedSelectorException|zmq.ZError.CtxTerminatedException e) {
                    return null;
                } catch (ZMQException|zmq.ZError.IOException|zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "recv", e);
                    logger.catching(Level.DEBUG, e.getCause());
                    ctx.close(listeningSocket);
                    listeningSocket = null;
                    return null;
                }
            }

        };
    }

    @Override
    public void close() {
        listeningSocket.close();
        super.close();
    }

    public String getMethod() {
        return method.toString();
    }

    public void setMethod(String method) {
        this.method = ZMQHelper.Method.valueOf(method.toUpperCase());
    }

    public String getListen() {
        return listen;
    }

    public void setListen(String endpoint) {
        this.listen = endpoint;
    }

    public int getHwm() {
        return hwm;
    }

    public void setHwm(int hwm) {
        this.hwm = hwm;
    }

    public String getType() {
        return type.toString();
    }

    public void setType(String type) {
        this.type = ZMQHelper.Type.valueOf(type.trim().toUpperCase());
    }

    @Override
    public String getReceiverName() {
        return "ZMQ";
    }

}
