package loghub;

import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;

public abstract class Processor {

    protected final Logger logger;

    private String[] path = new String[]{};
    private Expression ifexpression = null;
    private Processor success = null;
    private Processor failure = null;
    private Processor exception = null;
    private String ifsource = null;
    private String id = null;

    public Processor() {
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    public boolean configure(Properties properties) {
        logger.debug("configuring {}", this);
        if(ifsource != null) {
            try {
                ifexpression = new Expression(ifsource, properties.groovyClassLoader, properties.formatters);
            } catch (ExpressionException e) {
                Expression.logError(e, ifsource, logger);
                return false;
            }
        }
        if(success != null && ! success.configure(properties)) {
            return false;
        }
        if(failure != null && ! failure.configure(properties)) {
            return false;
        }
        if(exception != null && ! exception.configure(properties)) {
            return false;
        }
        return true;
    }

    public abstract boolean process(Event event) throws ProcessorException;

    public String getName() {
        return toString();
    }

    public void setPathArray(String[] path) {
        this.path = path;
    }

    public String[] getPathArray() {
        return path;
    }
    /**
     * @return the fieldprefix
     */
    public String getPath() {
        Optional<String> o = Arrays.stream(path).reduce( (i,j) -> i + "." + j);
        return o.isPresent() ? o.get() : "";
    }

    /**
     * @param fieldprefix the fieldprefix to set
     */
    public void setPath(String fieldprefix) {
        this.path = fieldprefix.split("\\.");
    }

    public void setIf(String ifsource) {
        this.ifsource = ifsource;
    }

    public String getIf() {
        return ifsource;
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
