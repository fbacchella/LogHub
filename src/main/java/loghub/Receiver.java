package loghub;

import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public abstract class Receiver extends Thread {
    protected final Socket pipe;
    private final Map<byte[], Event> eventQueue;
    protected Codec codec;

    public Receiver(Context context, String endpoint, Map<byte[], Event> eventQueue) {
        setDaemon(true);
        this.eventQueue = eventQueue;
        pipe = context.socket(ZMQ.PUSH);
        pipe.connect(endpoint);
    }

    public abstract void run();
    
    public void send(Event event) {
        try {
            byte[] key = event.key();
            pipe.send(key);
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

    public Codec getCodec() {
        return codec;
    }

    public void setCodec(Codec codec) {
        this.codec = codec;
    }
    
    public abstract String getReceiverName();

}
