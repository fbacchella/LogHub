package loghub.receivers;

import java.util.Date;
import java.util.Map;

import loghub.Event;
import loghub.Receiver;
import loghub.configuration.Beans;
import static org.zeromq.ZMQ.PULL;

import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

@Beans({"method", "endpoint"})
public class ZMQ extends Receiver {

    private Socket log4jsocket;
    private String method = "bind";
    private String endpoint = "tcp://localhost:2120";
    private int hwm = 1000;
    private Context context;

    @Override
    public synchronized void start() {
        log4jsocket = context.socket(PULL);
        log4jsocket.setHWM(hwm);
        switch (method.toLowerCase()) {
        case "bind": log4jsocket.bind(endpoint); break;
        case "connect": log4jsocket.connect(endpoint); break;
        }

        super.start();
    }    

    @Override
    public void configure(Context context, String endpoint,
            Map<byte[], Event> eventQueue) {
        super.configure(context, endpoint, eventQueue);
        this.context = context;
    }

    @Override
    public void run() {
        try {
            while (! isInterrupted()) {
                byte[] msg;
                try {
                    // Get work piece
                    msg = log4jsocket.recv();
                    Event event = new Event();
                    Date d = new Date();
                    event.timestamp = d;
                    codec.decode(event, msg);
                    send(event);
                } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                    // ZeroMQ throws exception
                    // when context is terminated
                    try {
                        log4jsocket.close();
                    } catch (Exception e1) {
                    }
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
