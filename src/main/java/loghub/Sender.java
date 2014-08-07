package loghub;

import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public abstract class Sender extends Thread {

    protected final ZMQ.Socket pipe;
    private final Map<String, Event> eventQueue;
    public Sender(Context ctx, String endpoint, Map<String, Event> eventQueue) {
        setDaemon(true);
        this.eventQueue = eventQueue;
        pipe = ctx.socket(ZMQ.PULL);
        pipe.connect(endpoint);
    }

    public abstract void send(Event e);

    public void run() {
        while (! isInterrupted()) {
            try {
                byte[] msg = pipe.recv(0);
                String key = new String(msg);
                Event event = eventQueue.remove(key);
                if(event == null) {
                    continue;
                }
                send(event);
            } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                // ZeroMQ throws exception
                // when context is terminated
                try {
                    pipe.close();
                } catch (Exception e1) {
                }
                break;
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}