package loghub;

import java.util.Map;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"threads"})
public abstract class Processor {

    private int threads = 1;
    private String fieldprefix = "";

    public Processor() {
    }

    public boolean configure(Properties properties) {
        return true;
    }

    public abstract void process(Event event);
    public abstract String getName();

    public void addElement(Event event, String key, Object value) {
        event.put(fieldprefix, key, value);
    }

    public void addElementAll(Event event, Map<String, Object> map) {
        map.entrySet().stream()
        .forEach( e -> event.put(fieldprefix, e.getKey(), e.getValue()));
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * @return the fieldprefix
     */
    public String getFieldprefix() {
        return fieldprefix;
    }

    /**
     * @param fieldprefix the fieldprefix to set
     */
    public void setFieldprefix(String fieldprefix) {
        this.fieldprefix = fieldprefix;
    }

}
