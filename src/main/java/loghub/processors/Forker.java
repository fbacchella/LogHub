package loghub.processors;

import java.util.concurrent.BlockingQueue;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;
import loghub.configuration.Properties;

/**
 * An empty processor, it's just a place holder. It should never be used directly
 * 
 * @author Fabrice Bacchella
 *
 */
public class Forker extends Processor {

    private String destination;
    private Pipeline pipeDestination;
    private BlockingQueue<Event> mainQueue;

    @Override
    public boolean process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    public boolean fork(Event event) {
        Event newEvent = event.duplicate();
        if(newEvent == null) {
            return false;
        }

        newEvent.inject(pipeDestination, mainQueue);
        return true;
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
            logger.error("invalid destination for forked event: {}", destination);
            return false;
        }
        pipeDestination = properties.namedPipeLine.get(destination);
        mainQueue = properties.mainQueue;
        return super.configure(properties);
    }

}
