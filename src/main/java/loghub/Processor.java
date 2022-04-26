package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;

public abstract class Processor {

    protected final Logger logger;

    private VariablePath path = VariablePath.EMPTY;
    private Expression ifexpression = null;
    private Processor success = null;
    private Processor failure = null;
    private Processor exception = null;
    private String ifsource = null;
    private String id = null;

    protected Processor() {
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
    }

    protected Processor(Logger logger) {
        this.logger = logger;
    }

    public boolean configure(Properties properties) {
        logger.debug("configuring {}", this);
        if (success != null && ! success.configure(properties)) {
            return false;
        }
        if( failure != null && ! failure.configure(properties)) {
            return false;
        }
        if (exception != null && ! exception.configure(properties)) {
            return false;
        }
        return true;
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
        return path.toString();
    }

    /**
     * @param fieldprefix the fieldprefix to set
     */
    public void setPath(String fieldprefix) {
        this.path = VariablePath.of(VariablePath.pathElements(fieldprefix));
    }

    public void setIf(Expression ifexpression) {
        this.ifexpression = ifexpression;
    }

    public Expression getIf() {
        return ifexpression;
    }

    public boolean isprocessNeeded(Event event) throws ProcessorException {
        if(ifexpression == null) {
            return true;
        } else {
            Object status = ifexpression.eval(event);
            if(status == null) {
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
