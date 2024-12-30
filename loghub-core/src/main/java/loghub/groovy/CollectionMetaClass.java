package loghub.groovy;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import groovy.lang.MetaClass;

public class CollectionMetaClass extends LoghubMetaClass<CollectionMetaClass.IterableHandler> {

    protected static class IterableHandler {
        private final Collection<Object> collection;
        private final Object array;
        private final Consumer<Collection<Object>> filler;
        private IterableHandler(Collection<Object> collection) {
            this.collection = collection;
            this.filler = c -> c.addAll(collection);
            this.array = null;
        }
        private IterableHandler(Object array) {
            this.array = array;
            this.filler = this::arrayFiller;
            this.collection = null;
        }
        private void arrayFiller(Collection<Object> c) {
            if (c instanceof ArrayList) {
                ArrayList<Object> al = (ArrayList<Object>) c;
                al.ensureCapacity(al.size() + Array.getLength(array));
            }
            if (Object[].class.isAssignableFrom(array.getClass())) {
                Collections.addAll(c, (Object[]) array);
            } else if (array instanceof double[]) {
                for (double b : (double[]) array) {
                    c.add(b);
                }
            } else if (array instanceof float[]) {
                for (float b : (float[]) array) {
                    c.add(b);
                }
            } else if (array instanceof long[]) {
                for (long b : (long[]) array) {
                    c.add(b);
                }
            } else if (array instanceof int[]) {
                for (int b : (int[]) array) {
                    c.add(b);
                }
            } else if (array instanceof short[]) {
                for (short b : (short[]) array) {
                    c.add(b);
                }
            } else if (array instanceof byte[]) {
                for (byte b : (byte[]) array) {
                    c.add(b);
                }
            } else if (array instanceof boolean[]) {
                for (boolean b : (boolean[]) array) {
                    c.add(b);
                }
            }
        }
    }

    public CollectionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object callMethod(IterableHandler handler, GroovyMethods method, Object argument) {
        if (method == GroovyMethods.PLUS) {
            try {
                Collection<Object> c;
                // Explicit test because List.of() return a class with no public constructor
                if (handler.collection instanceof List || handler.array != null) {
                    c = new ArrayList<>();
                } else if (handler.collection instanceof Set) {
                    c = new LinkedHashSet<>();
                } else if (handler.collection != null) {
                    c = handler.collection.getClass().getConstructor().newInstance();
                } else {
                    throw new IllegalStateException("Unusable handler");
                }
                handler.filler.accept(c);
                if (! (argument instanceof Collection || argument.getClass().isArray())) {
                    c.add(argument);
                } else {
                    convertArgument(argument).filler.accept(c);
                }
                return c;
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                // Fails to resolve using a fast path, perhaps groovy will be smarter.
                return invokeMethod(handler, method, argument);
            }
        } else {
            return invokeMethod(handler, method, argument);
        }
    }

    @Override
    protected boolean isHandledClass(Object o) {
        return o instanceof Collection || o.getClass().isArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected IterableHandler convertArgument(Object object) {
        if (object.getClass().isArray()) {
            return new IterableHandler(object);
        } else {
            return new IterableHandler((Collection<Object>) object);
        }
    }
}
