package loghub.processors;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Getter;

public abstract class ObjectExtractor<T> extends Processor {

    public abstract void extract(Event event, T object);

    @Getter
    private String source;
    private final Class<T> clazz;

    protected ObjectExtractor() {
        super();
        this.clazz = getClassType();
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object o = event.remove(source);
        if (clazz.isAssignableFrom(o.getClass())) {
            @SuppressWarnings("unchecked")
            T type = (T) o;
            extract(event, type);
            return true;
        } else {
            throw event.buildException("can't extract "+  getClassType().getCanonicalName() + " from " + o.getClass().getCanonicalName());
        }
    }

    public void setSource(String source) {
        this.source = source;
    }

    protected abstract Class<T> getClassType();

}
