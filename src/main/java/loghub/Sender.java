package loghub;

import java.util.Map;

import loghub.ZMQManager.Method;
import loghub.ZMQManager.Type;

import org.zeromq.ZMQ;

public abstract class Sender extends Thread {

    protected ZMQ.Socket pipe;
    private Map<byte[], Event> eventQueue;
    
    public Sender() {
        setDaemon(true);
        setName("sender-" + getSenderName());
    }

    public void configure(String endpoint, Map<byte[], Event> eventQueue) {
        this.eventQueue = eventQueue;
        pipe = ZMQManager.newSocket(Method.CONNECT, Type.PULL, endpoint);
    }

    public abstract void send(Event e);
    public abstract String getSenderName();

    public void run() {
        while (! isInterrupted()) {
            try {
                byte[] msg = ZMQManager.recv(pipe);
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