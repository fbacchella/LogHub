package loghub.receivers;

import java.util.Date;
import java.util.Map;

import loghub.Event;
import loghub.Receiver;
import loghub.ZMQManager;
import loghub.configuration.Beans;

import org.zeromq.ZMQ.Socket;

@Beans({"method", "listen", "type"})
public class ZMQ extends Receiver {

    private Socket log4jsocket;
    private String method = "bind";
    private String listen = "tcp://localhost:2120";
    private ZMQManager.Type type = ZMQManager.Type.SUB;
    private int hwm = 1000;

    @Override
    public void start(Map<byte[], Event> eventQueue) {
        log4jsocket = ZMQManager.newSocket(ZMQManager.Method.valueOf(method.toUpperCase()), ZMQManager.Type.SUB, listen);
        log4jsocket.setHWM(hwm);
        log4jsocket.subscribe(new byte[] {});
        super.start(eventQueue);
    }    

    @Override
    public void run() {
        try {
            while (! isInterrupted()) {
                byte[] msg;
                try {
                    // Get work piece
                    msg = ZMQManager.recv(log4jsocket);
                    Event event = new Event();
                    Date d = new Date();
                    event.timestamp = d;
                    codec.decode(event, msg);
                    send(event);
                } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                    break;
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
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
        this.type = ZMQManager.Type.valueOf(type.trim().toUpperCase());
    }

    @Override
    public String getReceiverName() {
        return "ZMQ";
    }

}
