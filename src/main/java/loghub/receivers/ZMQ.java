package loghub.receivers;

import java.util.Date;
import java.util.Map;

import loghub.Event;
import loghub.Receiver;
import loghub.ZMQManager;
import loghub.configuration.Beans;

import org.zeromq.ZMQ.Socket;

@Beans({"method", "endpoint"})
public class ZMQ extends Receiver {

    private Socket log4jsocket;
    private String method = "bind";
    private String endpoint = "tcp://localhost:2120";
    private int hwm = 1000;

    @Override
    public synchronized void start() {
        log4jsocket = ZMQManager.newSocket(ZMQManager.Method.valueOf(method.toUpperCase()), ZMQManager.Type.PULL, endpoint);
        log4jsocket.setHWM(hwm);

        super.start();
    }    

    @Override
    public void configure(String endpoint,
            Map<byte[], Event> eventQueue) {
        super.configure(endpoint, eventQueue);
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

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public int getHwm() {
        return hwm;
    }

    public void setHwm(int hwm) {
        this.hwm = hwm;
    }

    @Override
    public String getReceiverName() {
        return "ZMQ";
    }

}
