package loghub.senders;

import java.util.concurrent.BlockingQueue;

import org.zeromq.ZMQ.Socket;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Properties;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper;
import zmq.socket.Sockets;

public class ZMQ extends Sender {

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String destination = "tcp://localhost:2120";
    private Sockets type = Sockets.PUB;
    private int hwm = 1000;
    private Socket sendsocket = null;
    private final SmartContext ctx = SmartContext.getContext();
    private volatile boolean running = false;

    public ZMQ(BlockingQueue<Event> inQueue) {
        super(inQueue);
    }

    @Override
    public boolean configure(Properties properties) {
        sendsocket = ctx.newSocket(method, type, destination);
        return super.configure(properties);
    }

    @Override
    public void run() {
        running = true;
        super.run();
    }

    @Override
    public void stopSending() {
        running = false;
        super.stopSending();
    }

    @Override
    public boolean send(Event event) {
        if (running && sendsocket != null) {
            byte[] msg = getEncoder().encode(event);
            sendsocket.send(msg);
            return true;
        } else {
            if (sendsocket != null) {
                ctx.close(sendsocket);
            }
            return false;
        }
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
        this.type = Sockets.valueOf(type.trim().toUpperCase());
    }

    @Override
    public String getSenderName() {
        return "ZMQ";
    }

}
