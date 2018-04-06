package loghub.processors;

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
public class Forwarder extends Processor {

    private String destination;
    private Pipeline pipeDestination;

    @Override
    public boolean process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    public void forward(Event event) {
        event.finishPipeline();
        event.refill(pipeDestination);

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
        return super.configure(properties);
    }

}
