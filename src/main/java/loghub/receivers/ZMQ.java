package loghub.receivers;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import loghub.Event;
import loghub.Receiver;
import loghub.configuration.Beans;
import zmq.ZMQHelper;

import org.zeromq.ZMQ.Socket;

@Beans({"method", "listen", "type"})
public class ZMQ extends Receiver {

    private Socket log4jsocket;
    private String method = "bind";
    private String listen = "tcp://localhost:2120";
    private ZMQHelper.Type type = ZMQHelper.Type.SUB;
    private int hwm = 1000;

    @Override
    public void start(Map<byte[], Event> eventQueue) {
        log4jsocket = ctx.newSocket(ZMQHelper.Method.valueOf(method.toUpperCase()), ZMQHelper.Type.SUB, listen);
        log4jsocket.setHWM(hwm);
        log4jsocket.subscribe(new byte[] {});
        super.start(eventQueue);
    }    

    @Override
    protected Iterator<Event> getIterator() {
        return new Iterator<Event>() {
            byte[] msg;
            @Override
            public boolean hasNext() {
                try {
                    msg = ctx.recv(log4jsocket);      
                    return true;
                } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                    return false;
                }
            }

            @Override
            public Event next() {
                Event event = new Event();
                Date d = new Date();
                event.timestamp = d;
                codec.decode(event, msg);
                return event;
            }
            
        };
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
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
