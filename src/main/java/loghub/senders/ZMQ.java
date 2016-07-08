package loghub.senders;

import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import loghub.Event;
import loghub.Sender;
import loghub.SmartContext;
import loghub.configuration.Beans;
import zmq.ZMQHelper;

@Beans({"method", "destination", "type", "hwm"})
public class ZMQ extends Sender {

    @SuppressWarnings("unused")
    private static final Logger logger = LogManager.getLogger();

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String destination = "tcp://localhost:2120";
    private ZMQHelper.Type type = ZMQHelper.Type.PUB;
    private int hwm = 1000;
    private Socket sendsocket;
    private final SmartContext ctx = SmartContext.getContext();

    public ZMQ(BlockingQueue<Event> inQueue) {
        super(inQueue);
    }

    @Override
    public boolean send(Event event) {
        byte[] msg = getEncoder().encode(event);
        sendsocket.send(msg);
        return true;
    }

    public String getMethod() {
        return method.toString();
    }

    public void setMethod(String method) {
        this.method = ZMQHelper.Method.valueOf(method.toUpperCase());
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String endpoint) {
        this.destination = endpoint;
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
    public String getSenderName() {
        return "ZMQ";
    }

    @Override
    public synchronized void start() {
        sendsocket = ctx.newSocket(method, type, destination);
        super.start();
    }

}
