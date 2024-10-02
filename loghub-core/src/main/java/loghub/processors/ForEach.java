package loghub.processors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation;

import loghub.NullOrMissingValue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;

public class ForEach extends Processor {

    private static class StepContext {
        List<Object> original;
        List<Object> processed;
    }

    private class EndIterate extends Processor {
        private final StepContext sc;
        private final int rank;

        private EndIterate(int rank, StepContext sc) {
            this.rank = rank;
            this.sc = sc;
        }

        @Override
        public boolean process(Event event) {
            sc.processed.set(rank, event.getAtPath(ForEach.this.collectionPath));
            return true;
        }
    }

    private class StepIterate extends Processor {
        private final StepContext sc;
        private final int rank;

        private StepIterate(int rank, StepContext sc) {
            this.rank = rank;
            this.sc = sc;
        }

        @Override
        public boolean process(Event event) {
            VariablePath vp = ForEach.this.collectionPath;
            Object content = sc.original.get(rank);
            event.putAtPath(vp, content);
            sc.original.set(rank, null);
            event.insertProcessor(new EndIterate(rank, sc));
            event.insertProcessor(new UnwrapEvent());
            event.insertProcessor(ForEach.this.subProcessor);
            event.insertProcessor(new WrapEvent(vp));
            return true;
        }
    }

    private class FinishIterate extends Processor {
        List<Object> processed;

        private FinishIterate(StepContext sc) {
            this.processed = sc.processed;
        }

        @Override
        public boolean process(Event event) {
            event.putAtPath(ForEach.this.collectionPath, processed);
            return true;
        }
    }

    private final Processor subProcessor;
    private final VariablePath collectionPath;

    public ForEach(VariablePath collectionPath, Processor subProcessor) {
        this.collectionPath = collectionPath;
        this.subProcessor = subProcessor;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object o = event.getAtPath(collectionPath);
        List<Object> l;
        if (o instanceof NullOrMissingValue || o == null) {
            return true;
        } else if (o.getClass().isArray()) {
            l = DefaultTypeTransformation.primitiveArrayToList(o);
        } else if (o instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> c = (Collection<Object>) o;
            l = new ArrayList<>(c);
        } else {
            event.insertProcessor(subProcessor);
            return true;
        }
        if (!l.isEmpty()) {
            StepContext sc = new StepContext();
            sc.original = l;
            sc.processed = new ArrayList<>(l.size());
            event.insertProcessor(new FinishIterate(sc));
            for (int i = 0; i < l.size(); i++) {
                sc.processed.add(null);
                event.insertProcessor(new StepIterate(i, sc));
            }
        }
        return true;
    }

}
