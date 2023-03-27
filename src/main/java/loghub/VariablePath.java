package loghub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import loghub.events.Event;

public abstract class VariablePath {
    
    private static final PathTree<Object, VariablePath> PATH_CACHE = new PathTree<>(VariablePath.of(""));
    private static final PathTree<Object, VariablePath> PATH_CACHE_INDIRECT = new PathTree<>(VariablePath.of(""));

    private VariablePath() {
    }

    public abstract String get(int index);

    public boolean isTimestamp() {
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
        final String[] path;
        VariableLength(String[] path) {
            this.path = path;
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
                    .map(s -> '"' + s + '"')
                    .collect(Collectors.joining(","))
                    );
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
            return TIMESTAMP.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TimeStamp;
        }
    }

    private static class Meta extends FixedLength {
        private final String key;
        private Meta(String key) {
            this.key = key;
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
            return Objects.hash(Meta.class, key);
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Meta)) {
                return false;
            } else {
                return key.equals(((Meta)obj).key);
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

        @Override
        public int hashCode() {
            return Objects.hash(Plain.class, Arrays.hashCode(path));
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Plain)) {
                return false;
            } else {
                return Arrays.equals(path, ((Plain)obj).path);
            }
        }
    }

    private static class Empty extends VariablePath {
        @Override
        public int length() {
            return 0;
        }
        @Override
        public VariablePath append(String element) {
            return PATH_CACHE.computeChildIfAbsent(new String[]{}, element, () -> new Plain(new String[] {element}));
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
            return EMPTY.hashCode();
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
            return ALLMETAS.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof AllMeta;
        }
    }

    public static final VariablePath TIMESTAMP = new TimeStamp();

    public static final VariablePath EMPTY = new Empty();

    public static final VariablePath ALLMETAS = new AllMeta();

    public static VariablePath ofContext(String[] path) {
        return new Context(Arrays.copyOf(path, path.length));
    }

    public static VariablePath ofContext(List<String> path) {
        return new Context(path.toArray(String[]::new));
    }

    public static VariablePath ofMeta(String meta) {
        return new Meta(meta);
    }

    public static VariablePath ofIndirect(Object[] path) {
        return PATH_CACHE_INDIRECT.computeIfAbsent(path, () -> new Indirect(Arrays.stream(path).map(Object::toString).toArray(String[]::new)));
    }

    public static VariablePath ofIndirect(String[] path) {
        return PATH_CACHE_INDIRECT.computeIfAbsent(path, () -> new Indirect(Arrays.copyOf(path, path.length)));
    }

    public static VariablePath ofIndirect(List<String> path) {
        return new Indirect(path.toArray(String[]::new));
    }

    /**
     * Parsed a path as a dotted notation (e.g. a.b.c)
     * @param path as a dotted notation
     * @return thew new VariablePath
     */
    public static VariablePath of(String path) {
        if (path.isBlank()) {
            return EMPTY;
        } else {
            String[] pathParsed = pathElements(path).toArray(String[]::new);
            return PATH_CACHE.computeIfAbsent(pathParsed, () -> new Plain(pathParsed));
        }
    }

    public static VariablePath of(Object[] path) {
        if (path.length == 0) {
            return EMPTY;
        } else {
            return PATH_CACHE.computeIfAbsent(path, () -> new Plain(Arrays.stream(path).map(Object::toString).toArray(String[]::new)));
        }
    }

    public static VariablePath of(String[] path) {
        if (path.length == 0) {
            return EMPTY;
        } else {
            return PATH_CACHE.computeIfAbsent(path, () -> new Plain(Arrays.copyOf(path, path.length)));
        }
    }

    public static VariablePath of(List<String> path) {
        if (path.isEmpty()) {
            return EMPTY;
        } else {
            return new Plain(path.toArray(String[]::new));
        }
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
