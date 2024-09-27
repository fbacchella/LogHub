package loghub.processors;

import java.util.Map;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Lambda;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Filter.Builder.class)
@FieldsProcessor.ProcessNullField
public class Filter extends TreeWalkProcessor {

    public static class Builder extends TreeWalkProcessor.Builder<Filter> {
        @Setter
        private Lambda lambda;
        public Builder() {
            this.setTraversal(TRAVERSAL_ORDER.DEPTH);
        }
        public Filter build() {
            return new Filter(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Expression filter;

    private Filter(Builder builder) {
        super(builder);
        filter = builder.lambda.getExpression();
    }

    @Override
    protected Object processLeaf(Event event, Object value) throws ProcessorException {
        if (Boolean.TRUE.equals(filter.eval(event, value))) {
            return RUNSTATUS.REMOVE;
        } else {
            return value;
        }
    }

    @Override
    protected Object processNode(Event event, Map<String, Object> value) throws ProcessorException {
        return processLeaf(event, value);
    }

}
