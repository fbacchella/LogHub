package loghub.processors;

import java.util.Map;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Event.Action;
import loghub.Expression;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;

public class FireEvent extends Processor {

    private Map<VariablePath, Expression> expressions;
    private String destination;
    private Pipeline pipeDestination;
    private PriorityBlockingQueue mainQueue;

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

    @Override
    public boolean process(Event event) throws ProcessorException {
        Event newEvent = Event.emptyEvent(ConnectionContext.EMPTY);
        for (Map.Entry<VariablePath, Expression> e: expressions.entrySet()) {
            Object value = e.getValue().eval(event);
            newEvent.applyAtPath(Action.PUT, e.getKey(), value);
        }
        newEvent.reinject(pipeDestination, mainQueue);
        return true;
    }

    @Override
    public String getName() {
        return "Fire";
    }

    /**
     * @return the fields
     */
    public Map<VariablePath, Expression> getFields() {
        return expressions;
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(Map<VariablePath, Expression> fields) {
        this.expressions = Map.copyOf(fields);
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

}
