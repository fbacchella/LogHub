package loghub.processors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Expression;
import loghub.Pipeline;
import loghub.Expression.ExpressionException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class FireEvent extends Processor {

    private Map<String[], String> fields;
    private Map<String[], Expression> expressions;
    private String destination;
    private Pipeline pipeDestination;
    private BlockingQueue<Event> mainQueue;

    @Override
    public boolean configure(Properties properties) {
        expressions = new HashMap<>(fields.size());
        for(Map.Entry<String[], String> i: fields.entrySet()) {
            try {
                Expression ex = new Expression(i.getValue(), properties.groovyClassLoader, properties.formatters);
                expressions.put(i.getKey(), ex);
            } catch (ExpressionException e) {
                logger.error("invalid expression for field {}: {}", i.getKey(), i.getValue());
                return false;
            }
        }
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
        for(Map.Entry<String[], Expression> e: expressions.entrySet()) {
            Object value = e.getValue().eval(event);
            newEvent.applyAtPath( (i, j, k) -> i.put(j, k), e.getKey(), value);
        }
        return newEvent.inject(pipeDestination, mainQueue);
    }

    @Override
    public String getName() {
        return "Fire";
    }

    /**
     * @return the fields
     */
    public Map<String[], String> getFields() {
        return fields;
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(Map<String[], String> fields) {
        this.fields = fields;
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
