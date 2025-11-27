package loghub.processors;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Flatten.Builder.class)
public class Flatten  extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Flatten> {
        public Flatten build() {
            setIterate(false);
            return new Flatten(this);
        }
    }

    public static Flatten.Builder getBuilder() {
        return new Flatten.Builder();
    }

    public Flatten(Builder builder) {
        super(builder);
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        return Expression.flatten(value);
     }

}
