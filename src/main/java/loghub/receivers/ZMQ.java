package loghub.receivers;

import java.nio.channels.ClosedSelectorException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.SmartContext;
import loghub.configuration.Beans;
import zmq.ZMQHelper;

@Beans({"method", "listen", "type", "hwm"})
public class ZMQ extends Receiver {

    private static final Logger logger = LogManager.getLogger();

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String listen = "tcp://localhost:2120";
    private ZMQHelper.Type type = ZMQHelper.Type.SUB;
    private int hwm = 1000;

    public ZMQ(BlockingQueue<Event> outQueue, Pipeline processors) {
        super(outQueue, processors);
    }

    @Override
    protected Iterator<Event> getIterator() {
        SmartContext ctx = SmartContext.getContext();
        Socket log4jsocket = ctx.newSocket(method, type, listen);
        log4jsocket.setHWM(hwm);
        if(type == ZMQHelper.Type.SUB){
            log4jsocket.subscribe(new byte[] {});
        }
        return new Iterator<Event>() {
            byte[] msg;
            @Override
            public boolean hasNext() {
                msg = null;
                if(! ctx.isRunning()) {
                    return false;
                }
                try {
                    logger.debug("listening");
                    msg = log4jsocket.recv();
                    logger.debug("received a message");
                    return true;
                } catch (ClosedSelectorException|zmq.ZError.CtxTerminatedException e) {
                    return false;
                } catch (ZMQException|ZMQException.IOException|zmq.ZError.IOException|zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "recv", e);
                    ctx.close(log4jsocket);
                    return false;
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    logger.catching(Level.DEBUG, e);
                    ctx.close(log4jsocket);
                    return false;
                }
            }

            @Override
            public Event next() {
                return decode(msg);
            }

        };
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
