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
import loghub.cloners.NotClonableException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.metrics.Stats.PipelineStat;
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
        Stream<Object> enumerator;
        Object values = event.removeAtPath(toSlice);
        if (values instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = ((Collection<Object>) values);
            if (collection.isEmpty()) {
                throw IgnoredEventException.INSTANCE;
            } else {
                enumerator = collection.stream();
            }
        } else if (values.getClass().isArray() && Object.class.isAssignableFrom(values.getClass().getComponentType())) {
            Object[] array = (Object[]) values;
            if (array.length == 0) {
                throw IgnoredEventException.INSTANCE;
            } else {
                enumerator = Arrays.stream(array);
            }
        } else {
            throw IgnoredEventException.INSTANCE;
        }
        try {
            Map<Object, List<Object>> newValues = new LinkedHashMap<>();
            for (Iterator<Object> it = enumerator.iterator(); it.hasNext();) {
                Object i = it.next();
                event.putAtPath(toSlice, i);
                Object key = bucket.eval(event.wrap(toSlice));
                newValues.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
            event.removeAtPath(toSlice);
            List<Event> newEvents = new ArrayList<>(newValues.size());
            for (List<Object> sub : newValues.values()) {
                Event ev = event.duplicate();
                if (flatten && sub.size() == 1) {
                    ev.putAtPath(toSlice, sub.getFirst());
                } else {
                    ev.putAtPath(toSlice, sub);
                }
                newEvents.add(ev);
            }
            // All new event generated without errors, can inject them
            for (Event ev: newEvents) {
                ev.reinject(event, mainQueue);
            }
        } catch (RuntimeException | NotClonableException ex) {
            event.doMetric(PipelineStat.EXCEPTION, ex);
        }
        event.putAtPath(toSlice, values);
        throw DiscardedEventException.INSTANCE;
    }

}
