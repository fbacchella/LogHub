package loghub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class PipeStep extends Thread {

    protected Socket in;
    protected Socket out;
    private Map<byte[], Event> eventQueue;

    private final List<Transformer> transformers = new ArrayList<>();

    public PipeStep(int numStep, int width) {
        setDaemon(true);
        setName("pipe." + numStep + "." + width);
    }

    public void start(Map<byte[], Event> eventQueue, Context context, String endpointIn, String endpointOut) {
        this.eventQueue = eventQueue;
        in = context.socket(ZMQ.PULL);
        in.connect(endpointIn);

        out = context.socket(ZMQ.PUSH);
        out.connect(endpointOut);
        super.start();
    }

    public void addTransformer(Transformer t) {
        transformers.add(t);
    }
    
    public void run() {
        while (! isInterrupted()) {
            String threadName = getName();
            try {
                byte[] key = in.recv();
                Event event = eventQueue.remove(key);
                if(event == null) {
                    continue;
                }
                for(Transformer t: transformers) {
                    setName(threadName + "-" + t.getName());
                    t.transform(event);
                }
                out.send(key);
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
            } finally {
                setName(threadName);
            }
        }    
    }

    @Override
    public String toString() {
        return super.toString() + "." + transformers.toString();
    }
    
    
}
