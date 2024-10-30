package loghub.processors;

import java.util.Map;

import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class TreeWalkProcessor extends FieldsProcessor {

    @Setter
    public abstract static class Builder<WP extends TreeWalkProcessor> extends FieldsProcessor.Builder<WP> {
        private TRAVERSAL_ORDER traversal = TRAVERSAL_ORDER.BREADTH;
    }

    private final TRAVERSAL_ORDER traversal;

    TreeWalkProcessor(Builder<?> builder) {
        super(builder);
        this.traversal = builder.traversal;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value instanceof Map) {
            return processNode(event, (Map<String, Object>)value);
        } else {
            return processLeaf(event, value);
        }
     }

    protected abstract Object processLeaf(Event event, Object value) throws ProcessorException;

    protected abstract Object processNode(Event event, Map<String, Object> value) throws ProcessorException ;

}
