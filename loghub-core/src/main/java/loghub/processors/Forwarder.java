package loghub.processors;

import loghub.Pipeline;
import loghub.Processor;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * An empty processor, it's just a place holder. It should never be used directly
 *
 * @author Fabrice Bacchella
 *
 */
public class Forwarder extends Processor {

    @Setter
    @Getter
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

    @Override
    public boolean configure(Properties properties) {
        if (! properties.namedPipeLine.containsKey(destination)) {
            logger.error("invalid destination for forked event: {}", destination);
            return false;
        }
        pipeDestination = properties.namedPipeLine.get(destination);
        return super.configure(properties);
    }

}
