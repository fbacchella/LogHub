package loghub;

import java.util.Map;

import loghub.configuration.Beans;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

@Beans({"threads"})
public abstract class Transformer extends Thread {

    protected Socket in;
    protected Socket out;
    private final Map<String, Event> eventQueue;

    public Transformer(Map<String, Event> eventQueue) {
        setDaemon(true);

        this.eventQueue = eventQueue;

    }
    
    public void start(Context context, String endpointIn, String endpointOut) {
        in = context.socket(ZMQ.PULL);
        in.connect(endpointIn);

        out = context.socket(ZMQ.PUSH);
        out.connect(endpointOut);
        super.start();
    }

    public void run() {
        while (! isInterrupted()) {
            try {
                byte[] msg = in.recv();
                String key = new String(msg);
                Event event = eventQueue.remove(key);
                if(event == null) {
                    continue;
                }
                transform(event);
                out.send(key.getBytes());
                eventQueue.put(key, event);
            } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
                try {
                    in.close();
                } catch (Exception e1) {
                }
                try {
                    out.close();
                } catch (Exception e1) {
                }
                break;
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }    
    }

    public abstract void transform(Event event);

}
