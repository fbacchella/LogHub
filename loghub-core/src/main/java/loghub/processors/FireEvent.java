package loghub.processors;

import java.util.Map;

import loghub.Expression;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import lombok.Getter;
import lombok.Setter;

public class FireEvent extends Processor {

    private Map<VariablePath, Expression> expressions;
    @Setter
    @Getter
    private String destination;
    private Pipeline pipeDestination;
    private PriorityBlockingQueue mainQueue;
    private EventsFactory factory;

    @Override
    public boolean configure(Properties properties) {
        if( ! properties.namedPipeLine.containsKey(destination)) {
            logger.error("invalid destination for forked event: {}", destination);
            return false;
        }
        pipeDestination = properties.namedPipeLine.get(destination);
        mainQueue = properties.mainQueue;
        factory = properties.eventsFactory;
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Event newEvent = factory.newEvent();
        for (Map.Entry<VariablePath, Expression> e: expressions.entrySet()) {
            Object value = e.getValue().eval(event);
            newEvent.putAtPath(e.getKey(), value);
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

}
