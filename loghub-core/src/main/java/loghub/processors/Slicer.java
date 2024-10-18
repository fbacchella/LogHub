package loghub.processors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import loghub.BuilderClass;
import loghub.DiscardedEventException;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Slicer.Builder.class)
public class Slicer extends Processor {

    private static final AtomicLong unicityProvider = new AtomicLong();

    @Setter
    public static class Builder extends Processor.Builder<Slicer> {
        private Expression bucket = new Expression("unicityProvider", ed -> unicityProvider.incrementAndGet());
        private VariablePath toSlice;
        private boolean flatten = false;
        public Slicer build() {
            return new Slicer(this);
        }
    }
    public static Slicer.Builder getBuilder() {
        return new Slicer.Builder();
    }

    private final Expression bucket;
    private final VariablePath toSlice;
    private final boolean flatten;
    private PriorityBlockingQueue mainQueue;

    public Slicer(Slicer.Builder builder) {
        this.bucket = builder.bucket;
        this.toSlice = builder.toSlice;
        this.flatten = builder.flatten;
    }

    @Override
    public boolean configure(Properties properties) {
        mainQueue = properties.mainQueue;
        return true;
    }

    @SuppressWarnings("java:S3776")
    @Override
    public boolean process(Event event) throws ProcessorException {
        Map<Object, Event> events = new LinkedHashMap<>();
        Stream<Object> enumerator;
        Object values = event.getAtPath(toSlice);
        if (values instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = ((Collection<Object>)values);
            if (collection.size() <= 1) {
                throw IgnoredEventException.INSTANCE;
            } else {
                enumerator = collection.stream();
            }
        } else if (values.getClass().isArray() && Object.class.isAssignableFrom(values.getClass().getComponentType())) {
            Object[] array = (Object[]) values;
            if (array.length <= 1) {
                throw IgnoredEventException.INSTANCE;
            } else {
                enumerator = Arrays.stream(array);
            }
        } else {
            throw IgnoredEventException.INSTANCE;
        }
        Iterator<Object> iter = enumerator.iterator();
        while (iter.hasNext()) {
            Object i = iter.next();
            event.putAtPath(toSlice, i);
            Object key = bucket.eval(event.wrap(toSlice));
            Event newEvent = events.computeIfAbsent(key, k -> buildNewEvent(event));
            ((List<Object>) newEvent.getAtPath(toSlice)).add(i);
        }
        for (Event ev: events.values()) {
            if (flatten) {
                List<Object> sub = ((List<Object>) ev.getAtPath(toSlice));
                if (sub.size() == 1) {
                    ev.putAtPath(toSlice, sub.get(0));
                }
            }
            ev.reinject(event, mainQueue);
        }
        throw DiscardedEventException.INSTANCE;
    }

    private Event buildNewEvent(Event oldEvent) {
        try {
            Event newEvent;
            newEvent = oldEvent.duplicate();
            newEvent.putAtPath(toSlice, new ArrayList<>());
            return newEvent;
        } catch (ProcessorException e) {
            throw IgnoredEventException.INSTANCE;
        }
    }

}
