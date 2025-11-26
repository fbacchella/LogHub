package loghub.processors;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class TreeWalkProcessor extends FieldsProcessor {

    @Setter
    public abstract static class Builder<WP extends TreeWalkProcessor> extends FieldsProcessor.Builder<WP> {
        private TRAVERSAL_ORDER traversal = TRAVERSAL_ORDER.BREADTH;
    }

    private final TRAVERSAL_ORDER traversal;

    TreeWalkProcessor(Builder<?> builder) {
        super(builder);
        this.traversal = builder.traversal;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value instanceof Map<?, ?> rawMap) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) rawMap;
            return processNode(event, map);
        } else {
            return handleLeaf(event, value);
        }
    }

    private Object handleLeaf(Event event, Object value) throws ProcessorException {
        if (isIterate() && value instanceof Collection<?> c) {
            Stream<Object> s = c.stream().map(i -> {
                try {
                    return processLeaf(event, i);
                } catch (ProcessorException e) {
                    return NullOrMissingValue.MISSING;
                }
            });
            if (c instanceof List<?>) {
                return s.toList();
            } else if (c instanceof Set<?>) {
                return s.collect(Collectors.toSet());
            } else {
                throw event.buildException("Unhandled collect " + c.getClass());
            }
        } else if (isIterate() && value!= null && value.getClass().isArray()) {
            Object[] arr = (Object[]) value;
            Stream<Object> s = Arrays.stream(arr).map(i -> {
                try {
                    return processLeaf(event, i);
                } catch (ProcessorException e) {
                    return NullOrMissingValue.MISSING;
                }
            });
            return s.toList();
        } else {
            return processLeaf(event, value);
        }
    }

    protected abstract Object processLeaf(Event event, Object value) throws ProcessorException;

    protected abstract Object processNode(Event event, Map<String, Object> value) throws ProcessorException;

}
