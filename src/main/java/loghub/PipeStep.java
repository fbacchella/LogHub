package loghub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class PipeStep extends Thread {

    protected Socket in;
    protected Socket out;
    protected Map<byte[], Event> eventQueue;

    private final List<Transformer> transformers = new ArrayList<>();
 
    public PipeStep() {
        setDaemon(true);
    }

    public PipeStep(int numStep, int width) {
        setDaemon(true);
        setName("pipe." + numStep + "." + width);
    }

    public void start(Map<byte[], Event> eventQueue, String endpointIn, String endpointOut) {
        this.eventQueue = eventQueue;
        in = ZMQManager.newSocket(ZMQManager.Method.CONNECT, ZMQManager.Type.PULL, endpointIn);
        out = ZMQManager.newSocket(ZMQManager.Method.CONNECT, ZMQManager.Type.PUSH, endpointOut);
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
                    if(event.dropped) {
                        break;
                    }
                }
                if( ! event.dropped) {
                    eventQueue.put(key, event);                    
                    out.send(key);
                }
            } catch (ZMQException | ZMQException.IOException | zmq.ZError.IOException | zmq.ZError.CtxTerminatedException  | java.nio.channels.ClosedSelectorException  e ) {
                ZMQManager.close(out);
                ZMQManager.close(in);
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
    
    public ZMQManager.Type getInType() {
        return ZMQManager.Type.PUSH;
    }

    public ZMQManager.Type getOutType() {
        return ZMQManager.Type.PULL;
    }

}
