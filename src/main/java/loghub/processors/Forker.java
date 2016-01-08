package loghub.processors;

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;
import loghub.SmartContext;
import loghub.configuration.Properties;
import zmq.ZMQHelper;
import zmq.ZMQHelper.Method;
import zmq.ZMQHelper.Type;

/**
 * An empty processor, it's just a place holder. It should never be used directly
 * 
 * @author Fabrice Bacchella
 *
 */
public class Forker extends Processor {

    private static final Logger logger = LogManager.getLogger();

    private String destination;
    private Pipeline pipeDestination;
    private Socket destSocket;

    @Override
    public void process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    @Override
    public String getName() {
        return null;
    }

    public void fork(Event event, Map<byte[], Event> eventQueue) {
        if(destSocket == null){
            SmartContext ctx = SmartContext.getContext();
            destSocket = ctx.newSocket(Method.CONNECT, Type.PUSH, pipeDestination.inEndpoint);
            destSocket.setSndHWM(100);
            destSocket.setSendTimeOut(0);
        }

        Event newEvent = event.duplicate();
        if(newEvent == null) {
            return;
        }

        eventQueue.put(newEvent.key(), newEvent);
        try {
            destSocket.send(newEvent.key());
        } catch (RuntimeException ex) {
            logger.error(ex);
            ZMQHelper.logZMQException(logger, "forker", ex);
            logger.catching(Level.DEBUG, ex);
        }

    }

    /**
     * @return the destination
     */
    public String getDestination() {
        return destination;
    }

    /**
     * @param destination the destination to set
     */
    public void setDestination(String destination) {
        this.destination = destination;
    }

    @Override
    public boolean configure(Properties properties) {
        if( ! properties.namedPipeLine.containsKey(destination)) {
            logger.error("invalid destination for duplicate event: {}", destination);
            return false;
        }
        pipeDestination = properties.namedPipeLine.get(destination);
        return super.configure(properties);
    }

}
