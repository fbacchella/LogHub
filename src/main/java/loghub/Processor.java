package loghub;

import java.util.Arrays;
import java.util.Optional;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"threads"})
public abstract class Processor {

    private int threads = 1;
    private String[] path = new String[]{};

    public Processor() {
    }

    public boolean configure(Properties properties) {
        return true;
    }

    public abstract void process(Event event);
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

}
