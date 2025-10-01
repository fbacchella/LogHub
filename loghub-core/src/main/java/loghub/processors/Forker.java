package loghub.processors;

import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;
import lombok.Getter;
import lombok.Setter;

/**
 * An empty processor, it's just a placeholder. It should never be used directly
 *
 * @author Fabrice Bacchella
 *
 */
public class Forker extends Processor {

    @Setter
    @Getter
    private String destination;
    private Pipeline pipeDestination;
    private PriorityBlockingQueue mainQueue;

    @Override
    public boolean process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    public void fork(Event event) {
        logger.debug("Forking {} to {}", event, pipeDestination);
        try {
            Event newEvent = event.duplicate();
            newEvent.reinject(pipeDestination, mainQueue);
        } catch (ProcessorException ex) {
            logger.error("Failed to fork {}: {}", event, Helpers.resolveThrowableException(ex));
            event.doMetric(PipelineStat.EXCEPTION, ex);
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if (! properties.namedPipeLine.containsKey(destination)) {
            logger.error("invalid destination for forked event: {}", destination);
            return false;
        }
        pipeDestination = properties.namedPipeLine.get(destination);
        mainQueue = properties.mainQueue;
        return super.configure(properties);
    }

}
