package loghub;

import java.beans.FeatureDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import loghub.events.Event;

public abstract class VariablePath {

    private static final AtomicInteger VP_COUNT;

    public static final VariablePath EMPTY;
    public static final VariablePath ROOT;
    public static final VariablePath CURRENT;
    public static final VariablePath TIMESTAMP;
    public static final VariablePath LASTEXCEPTION;
    public static final VariablePath ALLMETAS;

    private static final PathTree<String, VariablePath>     PATH_CACHE;
    private static final PathTree<String, VariablePath>     PATH_CACHE_INDIRECT;
    private static final PathTree<String, VariablePath>     PATH_CACHE_CONTEXT;
    private static final Map<String, VariablePath>          PATH_CACHE_META;
    private static final Map<String, VariablePath>          PATH_CACHE_STRING;
    private static final AtomicReference<VariablePath[]>    PATH_CACHE_ID;
    private static final Map<Class<?>, Map<String, Method>> CONTEXT_BEANS = new ConcurrentHashMap<>();

    static {
        VP_COUNT = new AtomicInteger(0);
        PATH_CACHE_ID = new AtomicReference<>(new VariablePath[128]);
        EMPTY = new Empty();
        ROOT = new Root();
        CURRENT = new Current();
        TIMESTAMP = new TimeStamp();
        LASTEXCEPTION = new LastException();
        ALLMETAS = new AllMeta();
        PATH_CACHE = new PathTree<>(EMPTY);
        PATH_CACHE_INDIRECT = new PathTree<>(EMPTY);
        PATH_CACHE_CONTEXT = new PathTree<>(new Context(new String[]{}));
        PATH_CACHE_META = new ConcurrentHashMap<>();
        PATH_CACHE_STRING = new ConcurrentHashMap<>();
    }

    final int id;

    private VariablePath() {
        id = VP_COUNT.getAndIncrement();
        PATH_CACHE_ID.updateAndGet(v -> {
            VariablePath[] content;
            if ((id + 10) >= v.length) {
                content = Arrays.copyOf(v, (v.length + (v.length >> 1)));
            } else {
                content = v;
            }
            content[id] = this;
            return content;
        });
    }

    public String groovyExpression() {
        return "event.getGroovyPath(" + id + ")";
    }

    public abstract String get(int index);

    public boolean isTimestamp() {
        return false;
    }

    public boolean isException() {
        return false;
    }

    public boolean isIndirect() {
        return false;
    }

    public boolean isContext() {
        return false;
    }

    public boolean isMeta() {
        return false;
    }

    /**
     * Return a new {@link VariablePath} with a path element added
     * @param element the element to add
     * @return a new VariablePath with the element added
     */
    public abstract VariablePath append(String element);

    public abstract int length();

    public abstract String toString();

    private abstract static class FixedLength extends VariablePath {
        @Override
        public int length() {
            return 1;
        }
        @Override
        public VariablePath append(String element) {
            return this;
        }
    }

    private abstract static class VariableLength extends VariablePath {
        private final int hash;
        final String[] path;
        private final AtomicReference<Map<String, VariablePath>> childsRef = new AtomicReference<>();
        private VariableLength(String[] path) {
            this.path = path;
            this.hash = Objects.hash(getClass(), Arrays.hashCode(path));
        }
        @Override
        public int length() {
            return path.length;
        }
        @Override
        public VariablePath append(String element) {
            Map<String, VariablePath> childs = childsRef.updateAndGet(this::getCacheInstance);
            return childs.computeIfAbsent(element, this::findInCache);
        }
        private Map<String, VariablePath> getCacheInstance(Map<String, VariablePath> v) {
            return v == null ? new ConcurrentHashMap<>() : v;
        }
        VariablePath findInCache(String s) {
            return getCache().computeChildIfAbsent(path, s, () -> {
                String[] newPath = Arrays.copyOf(path, path.length + 1);
                newPath[newPath.length - 1] = s;
                return newInstance(newPath);
            });
        }
        abstract PathTree<String, VariablePath> getCache();
        @Override
        public String get(int index) {
            return path[index];
        }
        String smartPathPrint() {
            if (path.length == 1 && ".".equals(path[0])) {
                return ".";
            } else {
                StringJoiner joiner = new StringJoiner(" ");
                for (String s : path) {
                    char prefix = s.charAt(0);
                    boolean identifier = prefix == '.' || (Character.isJavaIdentifierStart(
                            prefix) && prefix != '$' && s.codePoints().allMatch(Character::isJavaIdentifierPart));
                    joiner.add(identifier ? s : '"' + s.replace("\"", "\\\"") + '"');
                }
                return joiner.toString();
            }
        }
        @Override
        public int hashCode() {
            return hash;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                return Arrays.equals(path, ((VariableLength) o).path);
            }
        }

        abstract VariablePath newInstance(String[] newPath);
    }

    private static class TimeStamp extends FixedLength {
        @Override
        public String toString() {
            return "[" + Event.TIMESTAMPKEY + "]";
        }

        @Override
        public boolean isTimestamp() {
            return true;
        }
        public String get(int index) {
            if (index == 0) {
                return Event.TIMESTAMPKEY;
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        }
        @Override
        public String groovyExpression() {
            return "event.getTimestamp()";
        }
        @Override
        public int hashCode() {
            return TimeStamp.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof TimeStamp;
        }
    }

    private static class LastException extends FixedLength {
        @Override
        public String toString() {
            return "[" + Event.LASTEXCEPTIONKEY + "]";
        }

        @Override
        public boolean isException() {
            return true;
        }
        public String get(int index) {
            if (index == 0) {
                return Event.LASTEXCEPTIONKEY;
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        }
        @Override
        public String groovyExpression() {
            return "event.getGroovyLastException()";
        }
        @Override
        public int hashCode() {
            return LastException.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof LastException;
        }
    }

    private static class Meta extends FixedLength {
        private final String key;
        private final int hash;
        private Meta(String key) {
            this.key = key;
            this.hash = Objects.hash(Meta.class, key);
        }
        @Override
        public String toString() {
            return "[#" + key + "]";
        }
        @Override
        public boolean isMeta() {
            return true;
        }
        public String get(int index) {
            if (index == 0) {
                return key;
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        }
        @Override
        public String groovyExpression() {
            return "event.getMeta(\"" + key + "\")";
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                return key.equals(((Meta) o).key);
            }
        }
    }

    private static class Context extends VariableLength {
        private Context(String[] path) {
            super(path);
        }
        private String pathSuffix() {
            return path.length == 0 ? "" :
                    '.' + String.join(".", path);
        }
        @Override
        public String toString() {
            return "[" + Event.CONTEXTKEY + pathSuffix() + "]";
        }
        @Override
        public boolean isContext() {
            return true;
        }
        @Override
        PathTree<String, VariablePath> getCache() {
            return PATH_CACHE_CONTEXT;
        }
        @Override
        VariablePath newInstance(String[] newPath) {
            return new Context(newPath);
        }
        @Override
        public String groovyExpression() {
            return "event.getConnectionContext()" + pathSuffix();
        }
        private Object resolve(Event ev) {
            Object o = ev.getConnectionContext();
            for (int i = 0; i < path.length; i++) {
                try {
                    o = beanResolver(o, path[i]);
                    // beanResolver return null before end of path, it's a missing value
                    if (o == null && i < (path.length - 1)) {
                        return NullOrMissingValue.MISSING;
                    }
                } catch (IllegalAccessException | InvocationTargetException ex) {
                    throw new IllegalArgumentException(String.format("Not a valid context path %s: %s", this, Helpers.resolveThrowableException(ex)), ex);
                }
            }
            return o;
        }
        private Object beanResolver(Object beanObject, String beanName)
                throws InvocationTargetException, IllegalAccessException {
            Method m = CONTEXT_BEANS.computeIfAbsent(beanObject.getClass(), c -> {
                try {
                    return Stream.of(Introspector.getBeanInfo(c, Object.class).getPropertyDescriptors())
                                   .filter(pd -> pd.getReadMethod() != null)
                                   .collect(Collectors.toMap(FeatureDescriptor::getName, PropertyDescriptor::getReadMethod));
                } catch (IntrospectionException e) {
                    return Collections.emptyMap();
                }
            }).get(beanName);
            if (m == null) {
                return NullOrMissingValue.MISSING;
            } else {
                return m.invoke(beanObject);
            }
        }
    }

    private static class Indirect extends VariableLength {
        private Indirect(String[] path) {
            super(path);
        }
        @Override
        public String toString() {
            return "[" + Event.INDIRECTMARK + ' ' + smartPathPrint() + "]";
        }
        @Override
        public boolean isIndirect() {
            return true;
        }
        @Override
        VariablePath newInstance(String[] newPath) {
            return new Indirect(newPath);
        }
        @Override
        PathTree<String, VariablePath> getCache() {
            return PATH_CACHE_INDIRECT;
        }
    }

    private static class Plain extends VariableLength {
        private Plain(String[] path) {
            super(path);
        }
        @Override
        public String toString() {
            return "[" + smartPathPrint() + "]";
        }
        @Override
        VariablePath newInstance(String[] newPath) {
            return new Plain(newPath);
        }
        @Override
        PathTree<String, VariablePath> getCache() {
            return PATH_CACHE;
        }
    }

    private static class Empty extends VariablePath {
        @Override
        public int length() {
            return 0;
        }
        @Override
        public VariablePath append(String element) {
            return VariablePath.of(element);
        }
        @Override
        public String get(int index) {
            throw new ArrayIndexOutOfBoundsException("Empty path");
        }
        @Override
        public String toString() {
            return "[]";
        }
        @Override
        public String groovyExpression() {
            return "event";
        }
        @Override
        public int hashCode() {
            return Empty.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof Empty;
        }
    }

    private static class Root extends VariablePath {
        @Override
        public int length() {
            return 1;
        }
        @Override
        public VariablePath append(String element) {
            return VariablePath.of(".", element);
        }
        @Override
        public String get(int index) {
            if (index == 0) {
                return ".";
            } else {
                throw new ArrayIndexOutOfBoundsException("Single element path");
            }
        }
        @Override
        public String toString() {
            return "[.]";
        }
        @Override
        public String groovyExpression() {
            return "event.getRealEvent()";
        }
        @Override
        public int hashCode() {
            return Root.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof Root;
        }
    }

    private static class Current extends VariablePath {
        @Override
        public int length() {
            return 1;
        }
        @Override
        public VariablePath append(String element) {
            return VariablePath.of(element);
        }
        @Override
        public String get(int index) {
            if (index == 0) {
                return "^";
            } else {
                throw new ArrayIndexOutOfBoundsException("Single element path");
            }
        }
        @Override
        public String toString() {
            return "[^]";
        }
        @Override
        public String groovyExpression() {
            return "event";
        }
        @Override
        public int hashCode() {
            return Current.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof Current;
        }
    }

    private static class AllMeta extends VariablePath {
        @Override
        public boolean isMeta() {
            return true;
        }
        @Override
        public int length() {
            return 0;
        }
        @Override
        public VariablePath append(String element) {
            return new Meta(element);
        }
        @Override
        public String get(int index) {
            throw new ArrayIndexOutOfBoundsException("Empty path");
        }
        @Override
        public String toString() {
            return "[#]";
        }
        @Override
        public String groovyExpression() {
            return "event.getMetas()";
        }
        @Override
        public int hashCode() {
            return AllMeta.class.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof AllMeta;
        }
    }

    public static VariablePath ofContext(String[] path) {
        return ofContext(Arrays.stream(path));
    }

    public static VariablePath ofContext(List<String> path) {
        return ofContext(path.stream());
    }

    public static VariablePath ofContext(Stream<String> path) {
        return PATH_CACHE_CONTEXT.computeIfAbsent(path, p -> new Context(p.toArray(String[]::new)));
    }

    public static VariablePath ofMeta(String meta) {
        return PATH_CACHE_META.computeIfAbsent(meta, Meta::new);
    }

    public static VariablePath ofIndirect(String[] path) {
        return ofIndirect(Arrays.stream(path));
    }

    public static VariablePath ofIndirect(List<String> path) {
        return ofIndirect(path.stream());
    }

    public static VariablePath ofIndirect(Stream<String> path) {
        return PATH_CACHE_INDIRECT.computeIfAbsent(path, p -> new Indirect(p.toArray(String[]::new)));
    }

    /**
     * Parsed a path as a dotted notation (e.g. a.b.c)
     * @param path as a dotted notation
     * @return thew new VariablePath
     */
    public static VariablePath parse(String path) {
        if (path.isBlank()) {
            return EMPTY;
        } else if (Event.TIMESTAMPKEY.equals(path)) {
            return TIMESTAMP;
        } else if (Event.LASTEXCEPTIONKEY.equals(path)) {
            return LASTEXCEPTION;
        } else if (path.startsWith("#")) {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofMeta(s.substring(1)));
        } else if (path.startsWith(Event.CONTEXTKEY)) {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofContext(pathElements(s.substring(Event.CONTEXTKEY.length()))));
        } else if (path.startsWith(Event.INDIRECTMARK)) {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofIndirect(pathElements(s.substring(Event.INDIRECTMARK.length()))));
        } else if (".".equals(path)) {
            return ROOT;
        } else if ("^".equals(path)) {
            return CURRENT;
        } else {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> {
                List<String> parts = pathElements(s);
                return PATH_CACHE.computeIfAbsent(parts, p -> new Plain(p.toArray(String[]::new)));
            });
        }
    }

    public static VariablePath of(String... path) {
        if (path.length == 1) {
            if ("^".equals(path[0])) {
                return CURRENT;
            } else if (".".equals(path[0])) {
                return ROOT;
            } else {
                return of(Arrays.stream(path));
            }
        } else {
            return of(Arrays.stream(path));
        }
    }

    public static VariablePath of(List<String> path) {
        if (path.size() == 1) {
            if ("^".equals(path.get(0))) {
                return CURRENT;
            } else if (".".equals(path.get(0))) {
                return ROOT;
            } else {
                return of(path.stream());
            }
        } else {
            return of(path.stream());
        }
    }

    public static VariablePath of(Stream<String> path) {
        return PATH_CACHE.computeIfAbsent(path, p -> new Plain(p.toArray(String[]::new)));
    }

    public static VariablePath of(VariablePath vp) {
        if (vp instanceof Indirect) {
            return VariablePath.of(((Indirect) vp).path);
        } else {
            return vp;
        }
    }

    public static List<String> pathElements(String path) {
        int curs = 0;
        int next;
        List<String> elements = new ArrayList<>();
        while ((next = path.indexOf('.', curs)) >= 0) {
            if (curs == 0 && next == 0) {
                elements.add(".");
                curs = 1;
            } else if (curs == next) {
                curs++;
            } else {
                elements.add(path.substring(curs, next));
                curs = next + 1;
            }
        }
        if (curs != path.length()) {
            elements.add(path.substring(curs));
        }
        return elements;
    }

    public static VariablePath getById(int id) {
        return PATH_CACHE_ID.get()[id];
    }

    /**
     * Compact the caches after parsing the configuration. All the {@link VariablePath} should be in the id cache
     */
    public static synchronized void compact() {
        PATH_CACHE.clear();
        PATH_CACHE_INDIRECT.clear();
    }

    public static Object resolveContext(Event ev, VariablePath vp) {
        if (!vp.isContext()) {
            throw new IllegalArgumentException("Not a context path");
        } else {
            return ((Context) vp).resolve(ev);
        }
    }

    /**
     * Used internally in tests
     */
    static synchronized void reset() {
        VP_COUNT.set(0);
        PATH_CACHE_ID.set(new VariablePath[128]);
        PATH_CACHE.clear();
        PATH_CACHE_INDIRECT.clear();
        PATH_CACHE_CONTEXT.clear();
        PATH_CACHE_META.clear();
        PATH_CACHE_STRING.clear();
        CONTEXT_BEANS.clear();
    }

}
