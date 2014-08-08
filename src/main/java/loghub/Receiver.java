package loghub;

import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public abstract class Receiver extends Thread {
    protected final Socket pipe;
    private final Map<String, Event> eventQueue;

    public Receiver(Context context, String endpoint, Map<String, Event> eventQueue) {
        setDaemon(true);
        this.eventQueue = eventQueue;
        pipe = context.socket(ZMQ.PUSH);
        pipe.connect(endpoint);
    }

    public abstract void run();
    
    public void send(Event event) {
        System.out.println("piping " + event.key());
        try {
            String key = event.key();
            pipe.send(key.getBytes());
            eventQueue.put(key, event);
        } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
            try {
                pipe.close();
            } catch (Exception e1) {
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}
