package loghub;

import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.processors.AnonymousSubPipeline;
import loghub.processors.UnstackException;
import lombok.Getter;
import lombok.Setter;

public abstract class Processor {

    public abstract static class Builder<B extends Processor> extends AbstractBuilder<B> {
        @Setter
        private VariablePath path = VariablePath.EMPTY;
        private Expression ifexpression = null;
        @Setter
        private Processor success = null;
        @Setter
        private Processor failure = null;
        @Setter
        private Processor exception = null;
        @Setter
        private String id = null;
        public void setIf(Expression ifexpression) {
            this.ifexpression = ifexpression;
        }
    }

    protected final Logger logger;

    private VariablePath path = VariablePath.EMPTY;
    private Expression ifexpression = null;
    @Getter
    private Processor success = null;
    @Getter
    private Processor failure = null;
    @Getter
    private Processor exception = null;
    @Getter
    private String id = null;

    protected Processor() {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
    }

    protected Processor(Logger logger) {
        this.logger = logger;
    }

    protected Processor(Builder<? extends Processor> builder) {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        path = Optional.ofNullable(builder.path)
                       .orElse(VariablePath.EMPTY);
        ifexpression = builder.ifexpression;
        success = builder.success;
        failure = builder.failure;
        if (builder.exception != null) {
            AnonymousSubPipeline asp = new AnonymousSubPipeline();
            List<Processor> processor = List.of(builder.exception, new UnstackException());
            asp.setPipeline(new Pipeline(processor, null, null));
            exception = asp;
        }
        id = builder.id;
    }

    public boolean configure(Properties properties) {
        logger.debug("configuring {}", this);
        if (success != null && ! success.configure(properties)) {
            return false;
        }
        if (failure != null && ! failure.configure(properties)) {
            return false;
        }
        return exception == null || exception.configure(properties);
    }

    public abstract boolean process(Event event) throws ProcessorException;

    public String getName() {
        return toString();
    }

    public void setPathArray(VariablePath path) {
        this.path = path;
    }

    public VariablePath getPathArray() {
        return path;
    }

    /**
     * @return the fieldprefix
     */
    public String getPath() {
        return path == null ? "" : path.toString();
    }

    /**
     * @param fieldprefix the fieldprefix to set
     */
    public void setPath(String fieldprefix) {
        this.path = VariablePath.parse(fieldprefix);
    }

    public void setIf(Expression ifexpression) {
        this.ifexpression = ifexpression;
    }

    public Expression getIf() {
        return ifexpression;
    }

    public boolean isprocessNeeded(Event event) throws ProcessorException {
        if (ifexpression == null) {
            return true;
        } else {
            Object status = ifexpression.eval(event);
            if (status == null) {
                return false;
            } else if (status instanceof Boolean) {
                return ((Boolean) status);
            } else if (status instanceof Number ){
                // If it's an integer, it will map 0 to false, any other value to true
                // A floating number will always return false, exact 0 don't really exist for float
                return ! "0".equals(status.toString());
            } else if (status instanceof String ){
                return ! ((String) status).isEmpty();
            } else {
                // a non empty object, it must be true ?
                return true;
            }
        }
    }

    /**
     * @param failure the failure to set
     */
    public void setFailure(Processor failure) {
        this.failure = failure;
    }

    /**
     * @param success the success to set
     */
    public void setSuccess(Processor success) {
        this.success = success;
    }

    /**
     * @param exception the success to set
     */
    public void setException(Processor exception) {
        this.exception = exception;
    }

    public void setId(String id) {
        this.id = id;
    }

    public interface ProcessEvent {
        boolean process(Event event) throws ProcessorException;
    }

    private static class LambdaProcessor extends Processor {
        private final ProcessEvent processEvent;
        public LambdaProcessor(Logger logger, ProcessEvent processEvent) {
            super(logger);
            this.processEvent = processEvent;
        }
        @Override
        public boolean process(Event event) throws ProcessorException {
            return processEvent.process(event);
        }

        @Override
        public boolean isprocessNeeded(Event event) {
            return true;
        }
    }

    public static Processor fromLambda(Processor holder, ProcessEvent pe) {
        return new LambdaProcessor(holder.logger, pe);
    }

}
