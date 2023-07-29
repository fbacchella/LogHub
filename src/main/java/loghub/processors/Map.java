package loghub.processors;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Lambda;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Map.Builder.class)
public class Map extends TreeWalkProcessor {

    public static class Builder extends TreeWalkProcessor.Builder<Map> {
        @Setter
        private Lambda lambda;
        public Map build() {
            return new Map(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Expression filter;

    private Map(Builder builder) {
        super(builder);
        filter = builder.lambda.getExpression();
    }

    @Override
    protected Object processLeaf(Event event, Object value) throws ProcessorException {
        return filter.eval(event, value);
    }

    @Override
    protected Object processNode(Event event, java.util.Map<String, Object> value) throws ProcessorException {
        return RUNSTATUS.NOSTORE;
    }

}
