package loghub;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"threads"})
public abstract class Processor {

    private int threads = 1;
    private String[] path = new String[]{};
    private Expression ifexpression = null;
    private String ifsource = null;

    public Processor() {
    }

    public boolean configure(Properties properties) {
        if(ifsource != null) {
            try {
                ifexpression = new Expression(ifsource, properties.groovyClassLoader);
            } catch (InstantiationException | IllegalAccessException e) {
                return false;
            }
        }
        return true;
    }

    public abstract void process(Event event) throws ProcessorException;
    public abstract String getName();

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
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

    public boolean isprocessNeeded(Event event) {
        if(ifexpression == null) {
            return true;
        } else {
            Object status = ifexpression.eval(event, Collections.emptyMap());
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
                System.out.println(status + " " + status.getClass().getCanonicalName());
                // a non empty object, it must be true ?
                return true;
            }
        }
    }

}
