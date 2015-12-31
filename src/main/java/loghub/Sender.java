package loghub;

import java.util.Map;

import org.zeromq.ZMQ;

import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

public abstract class Sender extends Thread {

    protected ZMQ.Socket pipe;
    private Map<byte[], Event> eventQueue;
    private String endpoint;
    protected final SmartContext ctx;
    
    public Sender() {
        setDaemon(true);
        setName("sender-" + getSenderName());
        ctx = SmartContext.getContext();
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public void configure(Map<byte[], Event> eventQueue) {
        this.eventQueue = eventQueue;
        pipe = ctx.newSocket(Method.CONNECT, Type.PULL, endpoint);
    }

    public abstract void send(Event e);
    public abstract String getSenderName();

    public void run() {
        while (! isInterrupted()) {
            try {
                byte[] msg = ctx.recv(pipe);
                Event event = eventQueue.remove(msg);
                if(event == null) {
                    continue;
                }
                send(event);
            } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                break;
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}