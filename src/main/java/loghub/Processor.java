package loghub;

import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.processors.AnonymousSubPipeline;
import loghub.processors.UnstackException;
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
    private Processor success = null;
    private Processor failure = null;
    private Processor exception = null;
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
        if( failure != null && ! failure.configure(properties)) {
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
        this.path = VariablePath.of(fieldprefix);
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
                return ! "0".equals(((Number)status).toString());
            } else if (status instanceof String ){
                return ! ((String) status).isEmpty();
            } else {
                // a non empty object, it must be true ?
                return true;
            }
        }
    }

    /**
     * @return the failure
     */
    public Processor getFailure() {
        return failure;
    }

    /**
     * @param failure the failure to set
     */
    public void setFailure(Processor failure) {
        this.failure = failure;
    }

    /**
     * @return the success
     */
    public Processor getSuccess() {
        return success;
    }

    /**
     * @param success the success to set
     */
    public void setSuccess(Processor success) {
        this.success = success;
    }

    /**
     * @return the exception
     */
    public Processor getException() {
        return exception;
    }

    /**
     * @param exception the success to set
     */
    public void setException(Processor exception) {
        this.exception = exception;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
