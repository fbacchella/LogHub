package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.BeansManager;
import loghub.events.Event;
import lombok.Data;
import lombok.Setter;

/**
 * A clean room implementation of Elastic dissector, the specs are available at <link href="https://github.com/elastic/dissect-specification>Dissect</link>
 *
 */
@BuilderClass(Dissect.Builder.class)
@FieldsProcessor.InPlace
public class Dissect extends FieldsProcessor {

    private enum Type {
        BYTE(Byte.TYPE),
        BOOLEAN(Boolean.TYPE),
        SHORT(Short.TYPE),
        INT(Integer.TYPE),
        INTEGER(Integer.TYPE),
        LONG(Long.TYPE),
        FLOAT(Float.TYPE),
        DOUBLE(Double.TYPE),
        IP(InetAddress .class);

        public final Class<?> destination;

        Type(Class<?> destination) {
            this.destination = destination;
        }
    }

    @Data
    private static class Key {
        private final boolean append;
        private final int appendModifier;
        private final boolean rpadding;
        private final boolean ignore;
        private final boolean refName;
        private final boolean refValue;
        private final boolean reference;
        private final String name;
        private final Class<?> classConverter;
        Key(String preflags, String name, String postflags, String appendModifier, String converter) {
            this.name = name;
            append = preflags != null && preflags.contains("+");
            ignore = name.isEmpty() || (preflags != null && preflags.contains("?"));
            refName = preflags != null && preflags.contains("*");
            refValue = preflags != null && preflags.contains("&");
            reference = refName || refValue;
            rpadding = postflags != null && postflags.contains("->");
            if (postflags != null && postflags.contains("/")) {
                this.appendModifier = Integer.parseInt(appendModifier);
            } else {
                this.appendModifier = -1;
            }
            if (! append && this.appendModifier >= 0) {
                throw new IllegalArgumentException("Append order defined, without append modifier");
            }
            try {
                classConverter = converter != null ? Type.valueOf(converter.toUpperCase(Locale.ENGLISH)).destination : null;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown conversion type: " +  converter);
            }
            if (classConverter != null && refName) {
                throw new IllegalArgumentException("A type conversion can't be applied to a reference name");
            }
        }
    }

    private static class AppendData {
        private int last = 0;
        private final String[] data;
        AppendData(int size) {
            data = new String[size];
        }
        AppendData(int size, String value) {
            data = new String[size];
            data[last++] = value;
        }
        void add(String value) {
            data[last++] = value;
        }
        void set(int pos, String value) {
            data[pos] = value;
        }
        String join(String separator) {
            return String.join(separator, data);
        }
    }

    @Data
    private static class Reference {
        final String key;
        String destination;
        Object value;
    }

    public static class Builder extends FieldsProcessor.Builder<Dissect> {
        @Setter
        String pattern;
        @Setter
        String appendSeparator = "";
        public Builder() {
            setInPlace(true);
        }
        public Dissect build() {
            return new Dissect(this);
        }
    }
    public static Dissect.Builder getBuilder() {
        return new Dissect.Builder();
    }

    private static final Pattern keyPattern = Pattern.compile("%\\{([+?*&])?([^}:]*?)(?::([a-z]+))?((?:->|/(\\d+)){0,2})}");

    private final String appendSeparator;
    private final List<Object> content = new ArrayList<>();
    private final Map<String, List<Key>> appendKeys;
    private final Set<String> referenceKeys;

    private Dissect(Dissect.Builder builder) {
        super(builder);
        appendSeparator = builder.appendSeparator;
        Matcher m = keyPattern.matcher(builder.pattern);
        Map<String, List<Key>> keysByName = new HashMap<>();
        int before = 0;
        int end = 0;
        while (m.find()) {
            int start = m.start();
            end = m.end();
            String beforeString = builder.pattern.substring(before, start);
            if (! beforeString.isEmpty()) {
                content.add(beforeString);
            }
            before = end;
            Key key = new Key(m.group(1), m.group(2), m.group(4), m.group(5), m.group(3));
            content.add(key);
            keysByName.computeIfAbsent(key.name, k -> new ArrayList<>()).add(key);
        }
        appendKeys = keysByName.entrySet()
                               .stream()
                               .filter(e -> keepAppender(e.getValue()))
                               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        referenceKeys = keysByName.entrySet()
                                  .stream()
                                  .filter(e -> keepReference(e.getValue())).map(Map.Entry::getKey)
                                  .collect(Collectors.toSet());
        String postString = builder.pattern.substring(end);
        if (! postString.isEmpty()) {
            content.add(postString);
        }
    }

    private boolean keepReference(List<Key> keys) {
        List<Key> tryReferenceKeys = keys.stream().filter(k -> k.reference).collect(Collectors.toList());
        if (tryReferenceKeys.isEmpty()) {
            return false;
        } else {
            if (tryReferenceKeys.size() != 2) {
                throw new IllegalArgumentException(String.format("Reference key \"%s\" must be specified as pair", keys.get(0).name));
            }
            if (tryReferenceKeys.get(0).refValue == tryReferenceKeys.get(1).refValue) {
                throw new IllegalArgumentException(String.format("Needs a reference name and a reference value for key \"%s\"", keys.get(0).name));
            } else {
                return true;
            }
        }
     }

    private boolean keepAppender(List<Key> keys) {
        if (keys.size() == 1) {
            return false;
        } else {
            for (Key k: keys) {
                if (k.appendModifier >= keys.size()) {
                    throw new IllegalArgumentException(String.format("Appender modifier out of range for key \"%s\"", k.name));
                }
                if (k.classConverter != null) {
                    throw new IllegalArgumentException(String.format("Appender only works with Strings for key \"%s\"", k.name));
                }
                if (k.ignore) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        String valueStr = value.toString();
        Map<String, Object> values = new HashMap<>();
        Map<String, Reference> references = referenceKeys.stream().map(Reference::new).collect(Collectors.toMap(r-> r.key, r -> r));
        int pos = 0;
        for (int i= 0; i < content.size(); i++) {
            boolean valid = true;
            Object o = content.get(i);
            if (o instanceof String) {
                String separator = (String) o;
                if (! valueStr.startsWith(separator, pos)) {
                    valid = false;
                } else {
                    pos += separator.length();
                }
            } else { // o instance of Key
                Key k = (Key) o;
                if (i + 1 < content.size()) {
                    String separator = (String) content.get(++i);
                    int nextseppos = valueStr.indexOf(separator, pos);
                    if (nextseppos < 0) {
                        valid = false;
                    } else {
                        String keyVal = valueStr.substring(pos, nextseppos);
                        pos = nextseppos + separator.length();
                        if (k.rpadding) {
                            while (valueStr.startsWith(separator, pos)) {
                                pos += separator.length();
                            }
                        }
                        valid = processKey(event, k, keyVal, values, references);
                    }
                } else {
                    valid = processKey(event, k, valueStr.substring(pos), values, references);
                }
            }
            if (! valid) {
                return RUNSTATUS.FAILED;
            }
        }
        for (Map.Entry<String, Object> e: values.entrySet()) {
            if (e.getValue() instanceof AppendData) {
                AppendData l = (AppendData) e.getValue();
                values.put(e.getKey(), l.join(appendSeparator));
            }
        }
        for (Reference r: references.values()) {
            values.put(r.destination, r.value);
        }
        if (isInPlace()) {
            for (Map.Entry<String, Object> e: values.entrySet()) {
                VariablePath vp = VariablePath.parse(e.getKey());
                event.putAtPath(vp, e.getValue());
            }
            return RUNSTATUS.NOSTORE;
        } else {
            return values;
        }
    }

    private boolean processKey(Event ev, Key key, String foundValue, Map<String, Object> values, Map<String, Reference> references)
            throws ProcessorException {
        Object value;
        if (key.classConverter != null && ! appendKeys.containsKey(key.name)) {
            // Append key only handle string values
            try {
                value = BeansManager.constructFromString(key.classConverter, foundValue);
            } catch (InvocationTargetException e) {
                throw ev.buildException(String.format("Can not convert \"%s\" to %s", key.name, key.classConverter), e);
            }
        } else {
            value = foundValue;
        }
        if (! key.ignore && ! key.append && ! key.reference) {
            values.put(key.name, value);
        } else if (key.refName) {
            references.get(key.name).destination = foundValue;
        } else if (key.refValue) {
            references.get(key.name).value = value;
        } else if (key.append) {
            AppendData valuesList = (AppendData) values.compute(key.name, (k, v) -> {
                if (v == null) {
                    return new AppendData(appendKeys.get(key.name).size());
                } else if (v instanceof AppendData) {
                    return v;
                } else {
                    return new AppendData(appendKeys.get(key.name).size(), v.toString());
                }
            });
            if (key.appendModifier > 0) {
                valuesList.set(key.appendModifier, foundValue);
            } else {
                valuesList.add(foundValue);
            }
        }
        return true;
    }

}
