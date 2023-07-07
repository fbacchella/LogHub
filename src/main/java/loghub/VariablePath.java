package loghub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import loghub.events.Event;

public abstract class VariablePath {

    public static final VariablePath EMPTY = new Empty();

    public static final VariablePath TIMESTAMP = new TimeStamp();

    public static final VariablePath LASTEXCEPTION = new LastException();

    public static final VariablePath ALLMETAS = new AllMeta();

    private static final PathTree<String, VariablePath> PATH_CACHE          = new PathTree<>(EMPTY);
    private static final PathTree<String, VariablePath> PATH_CACHE_INDIRECT = new PathTree<>(VariablePath.of(EMPTY));
    private static final Map<String, VariablePath>      PATH_CACHE_META     = new ConcurrentHashMap<>();
    private static final Map<String, VariablePath>      PATH_CACHE_STRING   = new ConcurrentHashMap<>();

    private VariablePath() {
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

    public abstract String groovyExpression();

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
            return PATH_CACHE.computeChildIfAbsent(path, element, () -> {
                String[] newPath = Arrays.copyOf(path, path.length + 1);
                newPath[newPath.length - 1] = element;
                return newInstance(newPath);
            });
        }
        @Override
        public String get(int index) {
            return path[index];
        }
        String smartPathPrint() {
            if (path.length == 1 && ".".equals(path[0])) {
                return ".";
            } else {
                StringJoiner joiner = new StringJoiner(".");
                for (int i=0; i < path.length; i++) {
                    if (i == 0 && ".".equals(path[i])) {
                        joiner.add("");
                    } else {
                        joiner.add(path[i]);
                    }
                }
                return joiner.toString();
            }
        }
        void getArguments(StringBuilder buffer) {
            buffer.append(Arrays.stream(path)
                    .map(s -> s.replace("'", "\\'"))
                    .map(s -> "'''" + s + "'''")
                    .collect(Collectors.joining(","))
                    );
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
                return Arrays.equals(path, ((VariableLength)o).path);
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
                return key.equals(((Meta)o).key);
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
        VariablePath newInstance(String[] newPath) {
            return new Context(newPath);
        }
        @Override
        public String groovyExpression() {
            return "event.getConnectionContext()" + pathSuffix();
        }
    }

    private static class Indirect extends VariableLength {
        private Indirect(String[] path) {
            super(path);
        }
        @Override
        public String toString() {
            return "[" + Event.INDIRECTMARK +' ' + smartPathPrint() + "]";
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
        public String groovyExpression() {
            StringBuilder buffer = new StringBuilder("event");
            buffer.append(".getGroovyIndirectPath(");
            getArguments(buffer);
            buffer.append(")");
            return buffer.toString();
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
        public String groovyExpression() {
            StringBuilder buffer = new StringBuilder("event");
            buffer.append(".getGroovyPath(");
            getArguments(buffer);
            buffer.append(")");
            return buffer.toString();
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
        return new Context(Arrays.copyOf(path, path.length));
    }

    public static VariablePath ofContext(List<String> path) {
        return new Context(path.toArray(String[]::new));
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
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofMeta(path.substring(1)));
        } else if (path.startsWith(Event.CONTEXTKEY)) {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofContext(pathElements(path.substring(Event.CONTEXTKEY.length()))));
        } else if (path.startsWith(Event.INDIRECTMARK)) {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.ofIndirect(pathElements(path.substring(Event.INDIRECTMARK.length()))));
        } else {
            return PATH_CACHE_STRING.computeIfAbsent(path, s -> VariablePath.of(pathElements(path)));
        }
    }

    public static VariablePath of(String... path) {
        return of(Arrays.stream(path));
    }

    public static VariablePath of(List<String> path) {
        return of(path.stream());
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
            } else if (curs == next){
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

}
