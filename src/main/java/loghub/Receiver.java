package loghub;

import java.util.Map;

import loghub.configuration.Beans;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

@Beans({"codec"})
public abstract class Receiver extends Thread {

    private static final Logger logger = LogManager.getLogger();

    protected Socket pipe;
    private Map<byte[], Event> eventQueue;
    protected Codec codec;
    private String endpoint;
    
    public Receiver(){
        setDaemon(true);
        setName("receiver-" + getReceiverName());
    }
    
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public void start(Map<byte[], Event> eventQueue) {
        this.eventQueue = eventQueue;
        pipe = ZMQManager.newSocket(ZMQManager.Method.CONNECT, ZMQManager.Type.PUSH, endpoint);
        super.start();
    }
    
    /**
     * reserved this class and prevent lower class use
     * @see java.lang.Thread#start()
     */
    @Override
    public final void start() {
        super.start();
    }

    public abstract void run();
    
    public void send(Event event) {
        try {
            logger.debug("new event: {}", event);
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
