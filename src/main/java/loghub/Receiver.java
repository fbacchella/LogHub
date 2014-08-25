package loghub;

import java.util.Map;

import loghub.configuration.Beans;

import org.zeromq.ZMQ.Socket;

@Beans({"codec"})
public abstract class Receiver extends Thread {
    protected Socket pipe;
    private Map<byte[], Event> eventQueue;
    protected Codec codec;

    public Receiver(){
        setDaemon(true);
        setName("receiver-" + getReceiverName());
    }
    
    public void configure(String endpoint, Map<byte[], Event> eventQueue) {
        this.eventQueue = eventQueue;
        pipe = ZMQManager.newSocket(ZMQManager.Method.CONNECT, ZMQManager.Type.PUSH, endpoint);
    }

    public abstract void run();
    
    public void send(Event event) {
        try {
            byte[] key = event.key();
            pipe.send(key);
            eventQueue.put(key, event);
        } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
            ZMQManager.close(pipe);
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
