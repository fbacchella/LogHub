package loghub.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import javax.net.ssl.SSLParameters;

import org.antlr.v4.runtime.RuleContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.Lambda;
import loghub.RouteParser;
import loghub.VariablePath;
import loghub.security.ssl.SslContextBuilder;
import lombok.Getter;

public class GrammarParserFiltering {

    private static final Logger logger = LogManager.getLogger();

    static final class LogHubClassloader extends URLClassLoader {
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
        OBJECT,
        IMPLICIT_OBJECT,
        MAP,
        LITERAL,
        SECRET,
        EXPRESSION,
        OPTIONAL_ARRAY,
        LAMBDA,
        VARIABLE_PATH,
        VARIABLE_PATH_ARRAY,
    }

    private static final Map<String, String> IMPLICIT_OBJECT = Map.ofEntries(
        Map.entry("http.sslContext", SslContextBuilder.class.getName()),
        Map.entry("http.sslParams", SSLParameters.class.getName()),
        Map.entry("sslContext", SslContextBuilder.class.getName()),
        Map.entry("sslParams", SSLParameters.class.getName()),
        Map.entry("jmx.sslContext", SslContextBuilder.class.getName()),
        Map.entry("jmx.sslParams", SSLParameters.class.getName())
    );

    private final Map<String, BEANTYPE> propertiesTypes = new HashMap<>();
    private final ArrayDeque<Class<?>> objectStack = new ArrayDeque<>();
    private BEANTYPE currentBeanType = null;
    @Getter
    private ClassLoader classLoader = getClass().getClassLoader();
    @Getter
    private final BeansManager manager = new BeansManager();
    @Getter
    private final Map<RouteParser.BeanValueContext, Class<?>> implicitObjets = new HashMap<>();
    private final Set<String> undeclaredProperties = new HashSet<>();

    public GrammarParserFiltering() {
        classLoader = GrammarParserFiltering.class.getClassLoader();
        refreshPropertiesTypes();
    }

    private void refreshPropertiesTypes() {
        undeclaredProperties.clear();
        Properties ptypes = new Properties();
        classLoader.resources("propertiestype.properties").forEach(p -> {
            try (InputStream is = p.openStream()) {
                ptypes.load(is);
            } catch (IOException e) {
                // Ignored
            }
        });
        ptypes.forEach((k, v) -> {
            String propName = k.toString();
            BEANTYPE propType = BEANTYPE.valueOf(v.toString().toUpperCase(Locale.US));
            propertiesTypes.put(propName, propType);
        });
    }

    public void enterObject(String objectName) {
        try {
            Class<?> objectClass = classLoader.loadClass(objectName);
            BuilderClass bca = objectClass.getAnnotation(BuilderClass.class);
            if (bca != null) {
                objectClass = bca.value();
            }
            objectStack.push(objectClass);
        } catch (ClassNotFoundException e) {
            // A generic class, the class is not found, will not try to resolve bean type
            objectStack.push(Object.class);
        }
    }

    public void exitObject() {
        objectStack.pop();
    }

    public void enterImplicitObject(String beanName) {
        currentBeanType = BEANTYPE.IMPLICIT_OBJECT;
        if (IMPLICIT_OBJECT.containsKey(beanName)) {
            enterObject(IMPLICIT_OBJECT.get(beanName));
        } else {
            throw new ConfigException("Not handled bean " + beanName);
        }
    }

    public void exitImplicitObject(RouteParser.BeanValueContext value) {
        currentBeanType = null;
        implicitObjets.put(value, objectStack.peek());
        exitObject();
    }

    public void resolveBeanType(String beanName) {
        Class<?> currentClass = objectStack.isEmpty() ? null : objectStack.peek();
        Method m;
        if (currentClass != null && ! Object.class.equals(currentClass)) {
            m = manager.getBean(objectStack.peek(), beanName);
        } else {
            m = null;
        }
        if (m != null) {
            Class<?> clazz = m.getParameterTypes()[0];
            if (clazz == Integer.TYPE || Integer.class.equals(clazz)) {
                currentBeanType = BEANTYPE.INTEGER;
            } else if (clazz == Double.TYPE || Double.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.FLOAT;
            } else if (clazz == Float.TYPE || Float.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.FLOAT;
            } else if (clazz == Byte.TYPE || Byte.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.INTEGER;
            } else if (clazz == Long.TYPE || Long.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.INTEGER;
            } else if (clazz == Short.TYPE || Short.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.INTEGER;
            } else if (clazz == Boolean.TYPE || Boolean.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.BOOLEAN;
            } else if (clazz == Character.TYPE || Character.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.CHARACTER;
            } else if (String.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.STRING;
            } else if (VariablePath[].class.equals(clazz)) {
                currentBeanType =  BEANTYPE.VARIABLE_PATH_ARRAY;
            } else if (clazz.isArray()) {
                currentBeanType =  BEANTYPE.ARRAY;
            } else if (clazz.isEnum()) {
                currentBeanType =  BEANTYPE.ENUM;
            } else if (Expression.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.EXPRESSION;
            } else if (Lambda.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.LAMBDA;
            } else if (VariablePath.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.VARIABLE_PATH;
            } else if (Map.class.isAssignableFrom(clazz)) {
                currentBeanType =  BEANTYPE.MAP;
            } else {
                currentBeanType =  BEANTYPE.OBJECT;
            }
        } else {
            currentBeanType =  null;
        }
    }

    public boolean allowedBeanType(BEANTYPE proposition) {
        switch (proposition) {
        case INTEGER:
            // A float bean can accept integer or float values
            return currentBeanType == null || currentBeanType == BEANTYPE.INTEGER || currentBeanType == BEANTYPE.FLOAT;
        case OPTIONAL_ARRAY:
            // Only allowed when explicitly required
            return currentBeanType == proposition;
        case STRING:
            // String is also valid for an Enum type or a VariablePath
            return currentBeanType == null || currentBeanType == proposition || currentBeanType == BEANTYPE.ENUM || currentBeanType == BEANTYPE.VARIABLE_PATH;
        case SECRET:
            return currentBeanType == null || currentBeanType == proposition || currentBeanType == BEANTYPE.STRING;
        case EXPRESSION:
            return currentBeanType != BEANTYPE.VARIABLE_PATH && (currentBeanType == null || currentBeanType == proposition);
        case IMPLICIT_OBJECT:
            return currentBeanType == BEANTYPE.IMPLICIT_OBJECT;
        case VARIABLE_PATH:
            // VARIABLE_PATH is valid only when explicitly required
            return currentBeanType == BEANTYPE.VARIABLE_PATH;
        case VARIABLE_PATH_ARRAY:
            // VARIABLE_PATH_ARRAY is valid only when explicitly required
            return currentBeanType == BEANTYPE.VARIABLE_PATH_ARRAY;
        default:
            return currentBeanType == null || currentBeanType == proposition;
        }
    }

    public void cleanBeanType() {
        currentBeanType = null;
    }

    public void checkProperty(String propertyName) {
        currentBeanType = propertiesTypes.get(propertyName);
        if (currentBeanType == null) {
            undeclaredProperties.add(propertyName);
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
            if (arrayContext == null) {
                classLoader = doClassLoader(new String[]{value.getText()}, classLoader);
            } else {
                String[] paths = arrayContext.arrayContent()
                                             .beanValue()
                                             .stream()
                                             .map(RuleContext::getText)
                                             .toArray(String[]::new);
                classLoader = doClassLoader(paths, classLoader);
            }
            Thread.currentThread().setContextClassLoader(classLoader);
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

}
