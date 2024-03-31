package loghub.configuration;

import java.beans.FeatureDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Lambda;
import lombok.Setter;

public class GrammarParserFiltering {

    Logger logger = LogManager.getLogger();

    private final Map<Class<?>, Map<String, Method>> beans = new HashMap<>();

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
        MAP,
        LITERAL,
        SECRET,
        EXPRESSION,
        OPTIONAL_ARRAY,
        LAMBDA,
    }

    private static final Map<String, BEANTYPE> PROPERTIES_TYPES = Map.ofEntries(
            Map.entry("hprofDumpPath", BEANTYPE.STRING),
            Map.entry("http.SSLKeyAlias", BEANTYPE.STRING),
            Map.entry("http.jaasName", BEANTYPE.STRING),
            Map.entry("http.jwt", BEANTYPE.BOOLEAN),
            Map.entry("http.port", BEANTYPE.INTEGER),
            Map.entry("http.listen", BEANTYPE.STRING),
            Map.entry("http.withSSL", BEANTYPE.BOOLEAN),
            Map.entry("http.withJolokia", BEANTYPE.BOOLEAN),
            Map.entry("http.jolokiaPolicyLocation", BEANTYPE.STRING),
            Map.entry("includes", BEANTYPE.OPTIONAL_ARRAY),
            Map.entry("plugins", BEANTYPE.OPTIONAL_ARRAY),
            Map.entry("ssl.trusts", BEANTYPE.OPTIONAL_ARRAY),
            Map.entry("ssl.issuers", BEANTYPE.OPTIONAL_ARRAY),
            Map.entry("ssl.context", BEANTYPE.STRING),
            Map.entry("ssl.providerclass", BEANTYPE.STRING),
            Map.entry("ssl.ephemeralDHKeySize", BEANTYPE.INTEGER),
            Map.entry("ssl.rejectClientInitiatedRenegotiation", BEANTYPE.INTEGER),
            Map.entry("ssl.keymanageralgorithm", BEANTYPE.STRING),
            Map.entry("ssl.trustmanageralgorithm", BEANTYPE.STRING),
            Map.entry("ssl.securerandom", BEANTYPE.STRING),
            Map.entry("secrets.source", BEANTYPE.STRING),
            Map.entry("timezone", BEANTYPE.STRING),
            Map.entry("locale", BEANTYPE.STRING),
            Map.entry("log4j.configFile", BEANTYPE.STRING),
            Map.entry("log4j.configURL", BEANTYPE.STRING),
            Map.entry("logfile", BEANTYPE.STRING),
            Map.entry("queueDepth", BEANTYPE.INTEGER),
            Map.entry("queueWeight", BEANTYPE.INTEGER),
            Map.entry("numWorkers", BEANTYPE.INTEGER),
            Map.entry("maxSteps", BEANTYPE.INTEGER),
            Map.entry("jmx.port", BEANTYPE.INTEGER),
            Map.entry("jmx.protocol", BEANTYPE.STRING),
            Map.entry("mibdirs", BEANTYPE.ARRAY),
            Map.entry("zmq.certsDirectory", BEANTYPE.STRING),
            Map.entry("zmq.keystore", BEANTYPE.STRING),
            Map.entry("zmq.linger", BEANTYPE.INTEGER),
            Map.entry("zmq.numSocket", BEANTYPE.INTEGER),
            Map.entry("zmq.withZap", BEANTYPE.BOOLEAN)
    );

    private final ArrayDeque<Class<?>> objectStack = new ArrayDeque<>();
    private BEANTYPE currentBeanType = null;
    @Setter
    private ClassLoader classLoader = this.getClass().getClassLoader();

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

    public void resolveBeanType(String beanName) {
        Class<?> currentClass = objectStack.isEmpty() ? null : objectStack.peek();
        Method m;
        if (currentClass != null && ! Object.class.equals(currentClass)) {
            m = beans.computeIfAbsent(objectStack.peek(), this::resolveBeans).get(beanName);
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
            } else if (clazz.isArray()) {
                currentBeanType =  BEANTYPE.ARRAY;
            } else if (clazz.isEnum()) {
                currentBeanType =  BEANTYPE.ENUM;
            } else if (Expression.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.EXPRESSION;
            } else if (Lambda.class.equals(clazz)) {
                currentBeanType =  BEANTYPE.LAMBDA;
            } else if (Map.class.isAssignableFrom(clazz)) {
                currentBeanType =  BEANTYPE.MAP;
            } else {
                currentBeanType =  BEANTYPE.OBJECT;
            }
        } else {
            currentBeanType =  null;
        }
    }

    private Map<String, Method> resolveBeans(Class<?> c) {
        try {
            Map<String, Method> introspectedBeans = Stream.of(Introspector.getBeanInfo(c, Object.class).getPropertyDescriptors())
                                                          .filter(pd -> pd.getWriteMethod() != null)
                                                          .collect(Collectors.toMap(FeatureDescriptor::getName, PropertyDescriptor::getWriteMethod));
            Map<String, Method> beans = new HashMap<>(introspectedBeans);
            Class<?> resolvingClass = c;
            while (resolvingClass != null) {
                BeansPostProcess annotation = resolvingClass.getAnnotation(BeansPostProcess.class);
                if (annotation != null) {
                    Class<? extends BeansPostProcess.Processor> processorClass = annotation.value();
                    BeansPostProcess.Processor processor = processorClass.getConstructor().newInstance();
                    processor.process(beans);
                }
                resolvingClass = resolvingClass.getSuperclass();
            }
            logger.debug("Found beans for {}: {}", c::getName, beans::keySet);
            return beans;
        } catch (IntrospectionException | InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            logger.atError().withThrowable(ex).log("Failed to resolve beans for {}", c::getName);
            return Map.of();
        }
    }

    public boolean allowedBeanType(BEANTYPE alternative) {
        switch (alternative) {
        case INTEGER:
            // A float bean can accept integer or float values
            return currentBeanType == null || currentBeanType == BEANTYPE.INTEGER || currentBeanType == BEANTYPE.FLOAT;
        case OPTIONAL_ARRAY:
            // Only allowed when explicitly required
            return currentBeanType == alternative;
        case STRING:
            // String is also valid for an Enum type
            return currentBeanType == null || currentBeanType == alternative || currentBeanType == BEANTYPE.ENUM;
        case SECRET:
            return currentBeanType == null || currentBeanType == alternative || currentBeanType == BEANTYPE.STRING;
        default:
            return currentBeanType == null || currentBeanType == alternative;
        }
    }

    public void cleanBeanType() {
        currentBeanType = null;
    }

    public void checkProperty(String propertyName) {
        currentBeanType = PROPERTIES_TYPES.get(propertyName);
    }
}
