package loghub.processors;

import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Log.Builder.class)
public class Log extends Processor {

    private final Expression message;
    private final Level level;

    public static class Builder extends Processor.Builder<Log> {
        @Setter
        String level;
        @Setter
        Expression message;
        public Log build() {
            return new Log(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    public Log(Builder builder) {
        super(builder);
        this.level = Level.toLevel(builder.level);
        this.message = builder.message;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        event.getPipelineLogger().log(level, message.eval(event));
        return true;
    }

    @Override
    public String getName() {
        return "log";
    }

}
