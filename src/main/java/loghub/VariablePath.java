package loghub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class VariablePath {
    
    private static final Map<String, VariablePath> pathCache = new ConcurrentHashMap<>();

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
     * Return an new {@link VariablePath} with a path element added
     * @param element
     * @return
     */
    public abstract VariablePath append(String element);

    public abstract int length();

    public abstract String toString();

    public abstract String rubyExpression();

    private abstract static class FixedLenght extends VariablePath {
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
            String[] newPath = Arrays.copyOf(path, path.length + 1);
            newPath[newPath.length - 1] = element;
            return newInstance(newPath);
        }
        @Override
        public String get(int index) {
            return path[index];
        }
        abstract VariablePath newInstance(String[] newPath);
    }

    private static class TimeStamp extends FixedLenght {
        @Override
        public String toString() {
            return Event.TIMESTAMPKEY;
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
        public String rubyExpression() {
            return "event.getTimestamp()";
        }
    }
    private static class Meta extends FixedLenght {
        private final String key;
        private Meta(String key) {
            this.key = key;
        }
        @Override
        public String toString() {
            return "#" + key;
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
        public String rubyExpression() {
            return "event.getMeta(\"" + key + "\")";
        }
    }
    private static class Context extends VariableLength {
        private Context(String[] path) {
            super(path);
        }
        @Override
        public String toString() {
            return Event.CONTEXTKEY +'.' + String.join(".", path);
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
        public String rubyExpression() {
            return "event.getConnectionContext()." + String.join(".", path);
        }
    }
    private static class Indirect extends VariableLength {
        private Indirect(String[] path) {
            super(path);
        }
        @Override
        public String toString() {
            return Event.INDIRECTMARK +' ' + String.join(".", path);
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
        public String rubyExpression() {
            StringBuilder buffer = new StringBuilder("event");
            buffer.append(".getIndirectPath(");
            buffer.append(Arrays.stream(path)
                    .map(s -> '"' + s + '"')
                    .collect(Collectors.joining(","))
                    );
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
            return String.join(".", path);
        }
        @Override
        VariablePath newInstance(String[] newPath) {
            return new Plain(newPath);
        }
        @Override
        public String rubyExpression() {
            StringBuilder buffer = new StringBuilder("event");
            buffer.append(".getPath(");
            buffer.append(Arrays.stream(path)
                    .map(s -> '"' + s + '"')
                    .collect(Collectors.joining(","))
                    );
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
            return new Plain(new String[] {element});
        }
        @Override
        public String get(int index) {
            throw new ArrayIndexOutOfBoundsException("Empty path");
        }
        @Override
        public String toString() {
            return "";
        }
        @Override
        public String rubyExpression() {
            return "";
        }
    }

    public static final VariablePath TIMESTAMP = new TimeStamp();

    public static final VariablePath EMPTY = new Empty();

    public static VariablePath ofContext(String[] path) {
        return new Context(Arrays.copyOf(path, path.length));
    }

    public static VariablePath ofContext(List<String> path) {
        return new Context(path.stream().toArray(String[]::new));
    }

    public static VariablePath ofMeta(String meta) {
        return new Meta(meta);
    }

    public static VariablePath ofIndirect(String[] path) {
        return new Indirect(Arrays.copyOf(path, path.length));
    }

    public static VariablePath ofIndirect(List<String> path) {
        return new Indirect(path.stream().toArray(String[]::new));
    }

    public static VariablePath of(String path) {
        return pathCache.computeIfAbsent(path, s -> new Plain(pathElements(s).stream().toArray(String[]::new)));
    }

    public static VariablePath of(String[] path) {
        return new Plain(Arrays.copyOf(path, path.length));
    }

    public static VariablePath of(List<String> path) {
        return new Plain(path.stream().toArray(String[]::new));
    }

    public static VariablePath of(VariablePath vp) {
        if (vp instanceof Indirect) {
            return new Plain(((Indirect) vp).path);
        } else {
            return vp;
        }
    }

    public static List<String> pathElements(String path) {
        int curs = 0;
        int next = 0;
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
        };
        if (curs != path.length()) {
            elements.add(path.substring(curs, path.length()));
        }
        return elements;
    }

}
