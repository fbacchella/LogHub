package loghub.configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.net.ssl.SSLParameters;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.Lambda;
import loghub.Processor;
import loghub.RouteParser;
import loghub.VariablePath;
import loghub.security.ssl.SslContextBuilder;
import lombok.Getter;

public class GrammarParserFiltering {

    private static final Logger logger = LogManager.getLogger();

    static class LogHubClassloader extends URLClassLoader {
        public LogHubClassloader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }
        public LogHubClassloader(URL[] urls) {
            super(urls);
        }
        @Override
        public String toString() {
            return "Loghub's class loader";
        }
    }

    static class LogHubRecognitionException extends RecognitionException {
        public LogHubRecognitionException(String message, Parser recognizer) {
            super(message, recognizer, recognizer.getInputStream(), recognizer.getContext());
            setOffendingToken(recognizer.getContext().getStart());
        }
    }

    public enum SECTION {
        INPUT,
        PIPELINE,
        OUTPUT,
    }

    public enum BEANTYPE {
        ENUM,
        INTEGER,
        ARRAY,
        FLOAT,
        BOOLEAN,
        CHARACTER,
        STRING,
        PATTERN,
        OBJECT,       // A loghub object
        OBJECT_CLASS, // An Object class
        IMPLICIT_OBJECT,
        MAP,
        LITERAL,
        SECRET,
        EXPRESSION,
        OPTIONAL_ARRAY,
        LAMBDA,
        VARIABLE_PATH,
        VARIABLE_PATH_LEGACY,
        VARIABLE_PATH_ARRAY,
        PROCESSOR,
        // A wild card type, typically used with properties
        ANY,
    }

    private static final Map<String, String> IMPLICIT_OBJECT = Map.ofEntries(
        Map.entry("http.sslContext", SslContextBuilder.class.getName()),
        Map.entry("http.sslParams", SSLParameters.class.getName()),
        Map.entry("sslContext", SslContextBuilder.class.getName()),
        Map.entry("sslParams", SSLParameters.class.getName()),
        Map.entry("jmx.sslContext", SslContextBuilder.class.getName()),
        Map.entry("jmx.sslParams", SSLParameters.class.getName())
    );

    private static final Set<String> LEGACY_FIELDS = Set.of("field", "destination", "path");

    private final Map<String, BEANTYPE> propertiesTypes;
    private final Map<String, BEANTYPE> propertiesArrayTypes;
    private final AtomicReference<ClassLoader> classLoaderHolder;
    @Getter
    private final BeansManager manager;
    @Getter
    private final Map<RouteParser.BeanValueContext, Class<?>> implicitObjets;
    private final Set<String> undeclaredProperties;
    private final Parser parser;

    private final ArrayDeque<Class<?>> objectStack = new ArrayDeque<>();
    private final ArrayDeque<BEANTYPE> beantypes = new ArrayDeque<>();
    private boolean inProperty = false;
    private BEANTYPE currentArrayType = null;

    public GrammarParserFiltering() {
        classLoaderHolder = new AtomicReference<>(GrammarParserFiltering.class.getClassLoader());
        undeclaredProperties = new HashSet<>();
        implicitObjets = new HashMap<>();
        propertiesTypes = new HashMap<>();
        propertiesArrayTypes = new HashMap<>();
        manager = new BeansManager();
        this.parser = null;
        refreshPropertiesTypes();
    }

    public GrammarParserFiltering(Parser parser, GrammarParserFiltering parent) {
        classLoaderHolder = parent.classLoaderHolder;
        undeclaredProperties = parent.undeclaredProperties;
        implicitObjets = parent.implicitObjets;
        propertiesTypes = parent.propertiesTypes;
        propertiesArrayTypes = parent.propertiesArrayTypes;
        manager = parent.manager;
        this.parser = parser;
    }

    private void refreshPropertiesTypes() {
        classLoaderHolder.get().resources("propertiestype.properties").forEach(this::readProperties);
    }

    private void readProperties(URL p) {
        try (InputStream is = p.openStream()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#") || line.startsWith("!")) {
                    continue;
                }
                String[] propkv = split(line, '=');
                if (propkv.length != 2) {
                    continue;
                }
                String propName = propkv[0];
                String propTypeValue = propkv[1];
                String[] propTypeSubtypeValue = split(propTypeValue, '/');
                BEANTYPE type = BEANTYPE.valueOf(propTypeSubtypeValue[0]);
                propertiesTypes.put(propName, type);
                if (propTypeSubtypeValue.length == 2) {
                    propertiesArrayTypes.put(propName, BEANTYPE.valueOf(propTypeSubtypeValue[1]));
                } else if (type == BEANTYPE.ARRAY || type == BEANTYPE.OPTIONAL_ARRAY) {
                    propertiesArrayTypes.put(propName, BEANTYPE.STRING);
                }
            }
        } catch (IOException ex) {
            logger.error("Failed to read {}", p);
        }
    }

    private String[] split(String s, char c) {
        int pos = s.indexOf(c);
        if (pos < 0) {
            return new String[]{s};
        } else {
            String s1 = s.substring(0, pos).strip();
            String s2 = s.substring(pos + 1).strip();
            return new String[]{s1, s2};
        }
    }

    public void enterObject(String objectIdentifier) {
        logger.debug("Entering object {}", objectIdentifier);
        try {
            Class<?> objectClass = classLoaderHolder.get().loadClass(objectIdentifier);
            BuilderClass bca = objectClass.getAnnotation(BuilderClass.class);
            if (bca != null) {
                objectClass = bca.value();
            }
            objectStack.push(objectClass);
        } catch (ClassNotFoundException e) {
            RecognitionException rex = new LogHubRecognitionException("Unknown object identifier '%s'".formatted(objectIdentifier), parser);
            parser.notifyErrorListeners(rex.getOffendingToken(), rex.getMessage(), rex);
        }
    }

    public void exitObject() {
        objectStack.pop();
    }

    public void enterBean(String beanName) {
        logger.debug("Enter bean {} in object stack {}", beanName, objectStack);
        if (IMPLICIT_OBJECT.containsKey(beanName)) {
            beantypes.push(BEANTYPE.IMPLICIT_OBJECT);
            enterObject(IMPLICIT_OBJECT.get(beanName));
        } else {
            Class<?> currentClass = objectStack.isEmpty() ? null : objectStack.peek();
            Method m;
            if (currentClass != null && ! Object.class.equals(currentClass)) {
                m = manager.getBean(objectStack.peek(), beanName);
            } else {
                m = null;
            }
            if (m != null) {
                Class<?> clazz = m.getParameterTypes()[0];
                if (VariablePath.class.equals(clazz) && LEGACY_FIELDS.contains(beanName)) {
                    // Only a few beans take a string for legacy configuration compatibility
                    beantypes.push(BEANTYPE.VARIABLE_PATH_LEGACY);
                } else {
                    beantypes.push(resolveType(clazz));
                }
            } else {
                beantypes.clear();
                RecognitionException rex = new LogHubRecognitionException("Unknown bean '%s'".formatted(beanName), parser);
                parser.notifyErrorListeners(rex.getOffendingToken(), rex.getMessage(), rex);
            }
        }
    }

    private BEANTYPE resolveType(Class<?> clazz) {
        if (clazz == Integer.TYPE || Integer.class.equals(clazz)) {
            return BEANTYPE.INTEGER;
        } else if (clazz == Double.TYPE || Double.class.equals(clazz)) {
            return  BEANTYPE.FLOAT;
        } else if (clazz == Float.TYPE || Float.class.equals(clazz)) {
            return  BEANTYPE.FLOAT;
        } else if (clazz == Byte.TYPE || Byte.class.equals(clazz)) {
            return  BEANTYPE.INTEGER;
        } else if (clazz == Long.TYPE || Long.class.equals(clazz)) {
            return  BEANTYPE.INTEGER;
        } else if (clazz == Short.TYPE || Short.class.equals(clazz)) {
            return  BEANTYPE.INTEGER;
        } else if (clazz == Boolean.TYPE || Boolean.class.equals(clazz)) {
            return  BEANTYPE.BOOLEAN;
        } else if (clazz == Character.TYPE || Character.class.equals(clazz)) {
            return BEANTYPE.CHARACTER;
        } else if (String.class.equals(clazz)) {
            return BEANTYPE.STRING;
        } else if (URI.class.equals(clazz)) {
            return BEANTYPE.STRING;
        } else if (clazz.isArray() && currentArrayType == null) {
            // Array can't be nested
            currentArrayType = resolveType(clazz.getComponentType());
            return BEANTYPE.ARRAY;
        } else if (clazz.isEnum()) {
            return BEANTYPE.ENUM;
        } else if (Expression.class.equals(clazz)) {
            return BEANTYPE.EXPRESSION;
        } else if (Lambda.class.equals(clazz)) {
            return BEANTYPE.LAMBDA;
        } else if (VariablePath.class.equals(clazz)) {
            return BEANTYPE.VARIABLE_PATH;
        } else if (Object.class.equals(clazz)) {
            return BEANTYPE.OBJECT_CLASS;
        } else if (Processor.class.isAssignableFrom(clazz)) {
            return BEANTYPE.PROCESSOR;
        } else if (Map.class.isAssignableFrom(clazz)) {
            return BEANTYPE.MAP;
        } else if (Pattern.class.isAssignableFrom(clazz)) {
            return BEANTYPE.PATTERN;
        } else {
            return BEANTYPE.OBJECT;
        }
    }

    public boolean allowedBeanType(BEANTYPE proposition) {
        BEANTYPE currentBeanType = beantypes.peek();
        return switch (currentBeanType) {
            case BEANTYPE.MAP ->
                    proposition != BEANTYPE.PROCESSOR && proposition != BEANTYPE.VARIABLE_PATH_LEGACY && proposition != BEANTYPE.VARIABLE_PATH_ARRAY && proposition != BEANTYPE.IMPLICIT_OBJECT && proposition != BEANTYPE.OPTIONAL_ARRAY;
            case BEANTYPE.IMPLICIT_OBJECT -> proposition == BEANTYPE.IMPLICIT_OBJECT;
            default -> currentBeanType == proposition || checkCompatibleType(currentBeanType, proposition);
        };
    }

    private boolean checkCompatibleType(BEANTYPE assignationType, BEANTYPE proposition) {
        // an Object as assignationType accepts any litteral
        if (assignationType == BEANTYPE.OBJECT_CLASS) {
            return proposition != BEANTYPE.IMPLICIT_OBJECT
                   && proposition != BEANTYPE.OBJECT
                   && proposition != BEANTYPE.LAMBDA
                   && proposition != BEANTYPE.OPTIONAL_ARRAY
                   && proposition != BEANTYPE.PROCESSOR;
        }
        // The cas were the proposition matches the expected type was already checked only compatible assignations
        // are checked here
        switch (proposition) {
        case EXPRESSION, LAMBDA, PROCESSOR:
            // some constructions make no sense in properties
            return assignationType == BEANTYPE.ANY && ! inProperty;
        case INTEGER:
            // A float bean can accept integer or float values
            return assignationType == BEANTYPE.FLOAT || assignationType == BEANTYPE.ANY;
        case STRING:
            // String is also valid for an Enum type
            // If currently in an array of VariablePath, a String is also accepted, for comptability for previous version
            // Pattern was previously defined using String, keep comptability
            return (currentArrayType == BEANTYPE.VARIABLE_PATH && assignationType == BEANTYPE.VARIABLE_PATH) ||
                    assignationType == BEANTYPE.ENUM ||
                    assignationType == BEANTYPE.PATTERN ||
                    assignationType == BEANTYPE.ANY;
        case SECRET:
            return assignationType == BEANTYPE.STRING || assignationType == BEANTYPE.ANY;
        case VARIABLE_PATH_ARRAY:
            return assignationType == BEANTYPE.VARIABLE_PATH_ARRAY || assignationType == BEANTYPE.VARIABLE_PATH;
        case VARIABLE_PATH:
            return assignationType == BEANTYPE.VARIABLE_PATH_LEGACY && ! inProperty;
        case IMPLICIT_OBJECT:
            // implicit type is allowed only when explicitely requested
            return false;
        default:
            // Default is to accept only itself, already checked, so it must fail now
            // unless if it's the jocker type
            return assignationType == BEANTYPE.ANY;
        }
    }

    public void exitBean(RouteParser.BeanValueContext value) {
        BEANTYPE previousType = beantypes.pop();
        if (previousType == BEANTYPE.IMPLICIT_OBJECT) {
            implicitObjets.put(value, objectStack.pop());
        }
        currentArrayType = null;
    }

    public void enterProperty(String propertyName) {
        if (IMPLICIT_OBJECT.containsKey(propertyName)) {
            beantypes.push(BEANTYPE.IMPLICIT_OBJECT);
            enterObject(IMPLICIT_OBJECT.get(propertyName));
        } else {
            beantypes.push(propertiesTypes.getOrDefault(propertyName, BEANTYPE.ANY));
            currentArrayType = propertiesArrayTypes.getOrDefault(propertyName, BEANTYPE.ANY);
        }
        if (! propertiesTypes.containsKey(propertyName)) {
            undeclaredProperties.add(propertyName);
        }
        inProperty = true;
    }

    public void exitProperty(RouteParser.BeanValueContext value) {
        inProperty = false;
        BEANTYPE previousType = beantypes.pop();
        if (previousType == BEANTYPE.IMPLICIT_OBJECT) {
            implicitObjets.put(value, objectStack.pop());
        }
    }

    public void checkUndeclaredProperties(Logger logger) {
        if (! undeclaredProperties.isEmpty()) {
            logger.warn("Unspecified properties, possible textual error: {}", () -> String.join(", ", undeclaredProperties));
        }
    }

    public void refreshIncludes(RouteParser.BeanValueContext value) {
        try {
            RouteParser.ArrayContext arrayContext = value.array();
            ClassLoader newClassLoader;
            if (arrayContext == null) {
                newClassLoader = doClassLoader(new String[]{value.getText()}, classLoaderHolder.get());
            } else {
                String[] paths = arrayContext.arrayContent()
                                             .beanValue()
                                             .stream()
                                             .map(RuleContext::getText)
                                             .toArray(String[]::new);
                newClassLoader = doClassLoader(paths, classLoaderHolder.get());
            }
            classLoaderHolder.set(newClassLoader);
            Thread.currentThread().setContextClassLoader(newClassLoader);
            refreshPropertiesTypes();
        } catch (IOException ex) {
            throw new ConfigException("Not valid includes path: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    ClassLoader doClassLoader(String[] pathElements, ClassLoader parent) throws IOException {
        Set<URL> includes = new LinkedHashSet<>();
        for (String s: pathElements) {
            Path p = Path.of(s);
            URL url = usablePathAsUrl(p);
            if (url != null) {
                includes.add(url);
                if (Files.isDirectory(p)) {
                    try (Stream<Path> files = Files.list(p)) {
                        files.map(this::usablePathAsUrl)
                             .filter(Objects::nonNull)
                             .forEach(includes::add);
                    }
                }
            }
        }
        return new LogHubClassloader(includes.toArray(new URL[] {}), parent);
    }

    private URL usablePathAsUrl(Path tryPath) {
        try {
            if ((! Files.isHidden(tryPath)) &&
                        Files.isReadable(tryPath) &&
                        ((Files.isRegularFile(tryPath) && tryPath.toString().endsWith(".jar")) || Files.isDirectory(tryPath))) {
                return tryPath.toUri().toURL();
            } else {
                return null;
            }
        } catch (IOException ex) {
            logger.atWarn()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Unusable plugin {}: {}", () -> tryPath, () -> Helpers.resolveThrowableException(ex));
            return null;
        }
    }

    public void enterArray() {
        beantypes.push(Optional.ofNullable(currentArrayType).orElse(BEANTYPE.ANY));
    }

    public void exitArray() {
        beantypes.pop();
    }

    public void enterMap() {
        beantypes.push(BEANTYPE.ANY);
    }

    public void exitMap() {
        beantypes.pop();
    }

    public ClassLoader getClassLoader() {
        return classLoaderHolder.get();
    }

}
