package loghub;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public abstract class Event extends HashMap<String, Object> implements Serializable {

    public static final String TIMESTAMPKEY = "@timestamp";

    public static Event emptyEvent() {
        return new EventInstance();
    }

    public static Event emptyTestEvent() {
        return new EventInstance(true);
    }

    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value, boolean create) {
        Map<String, Object> current = this;
        String key = path[0];
        for (int i = 0; i < path.length - 1; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if ( next == null || ! (next instanceof Map) ) {
                if (create) {
                    next = new HashMap<String, Object>();
                    current.put(path[i], next);
                } else {
                    return null;
                }
            }
            current = next;
            key = path[i + 1];
        }
        return f.apply(current, key, value);
    }

    /**
     * This method inject a new event in a pipeline as
     * a top processing pipeline. Not to be used for sub-processing pipeline
     * 
     * @param pipeline the pipeline with the processes to inject 
     * @param mainqueue the waiting queue
     * @return
     */
    public abstract boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue);

    public abstract Event duplicate();

    public abstract Processor next();

    public abstract void insertProcessor(Processor p);

    public abstract void appendProcessor(Processor p);

    public abstract void insertProcessors(List<Processor> p);

    public abstract void appendProcessors(List<Processor> p);

    public abstract String getCurrentPipeline();

    public abstract String getNextPipeline();

    public abstract boolean process(Processor p) throws ProcessorException;

    public abstract Date getTimestamp();

    public abstract void setTimestamp(Date timestamp);

    public abstract ProcessorException buildException(String message);

    public abstract ProcessorException buildException(String message, Exception root);

    public abstract void end();

    public abstract int stepsCount();
    
    public abstract boolean isTest();
    
    public abstract void doMetric(Runnable metric);
    
    public abstract void drop();

}
