package loghub.jackson;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import loghub.Helpers;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class JacksonBuilder<T extends ObjectMapper> {

    public static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { /* empty */ };

    @Setter
    JsonFactory factory = null;
    @Setter
    private FormatSchema schema = null;

    Set<Module> modules = new HashSet<>();

    Set<Enum<?>> features = new HashSet<>();
    @Setter @Singular
    Set<JsonSerializer<?>> serialiazers  = new HashSet<>();
    @Setter
    Supplier<T> mapperSupplier;
    @Setter
    TypeReference<?> typeReference = null;
    @Setter
    Consumer<T> configurator = null;

    public static <T extends ObjectMapper> JacksonBuilder<T> get(Class<T> clazz) {
        JacksonBuilder<T> jb = new JacksonBuilder<>();
        try {
            Constructor<T> cons = clazz.getConstructor();
            jb.mapperSupplier = () -> {
                try {
                    return cons.newInstance();
                } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                        | InvocationTargetException ex) {
                    throw new IllegalStateException("Unusable Jackson mapper " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex), ex);
                }
            };
        } catch (NoSuchMethodException | SecurityException ex) {
            throw new IllegalStateException("Unusable Jackson mapper " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex), ex);
        }
        return jb;
    }

    public static JacksonBuilder<ObjectMapper> get() {
        return new JacksonBuilder<>();
    }

    private JacksonBuilder() {
    }

    public ObjectReader getReader() {
        ObjectReader reader = getMapper().readerFor(typeReference != null ? typeReference: OBJECTREF);
        if (schema != null) {
            reader = reader.with(schema);
        }
        return reader;
    }

    public ObjectWriter getWriter() {
        ObjectWriter writer = getMapper().writerFor(typeReference != null ? typeReference: OBJECTREF);
        if (schema != null) {
            writer = writer.with(schema);
        }
        return writer;
    }

    @SuppressWarnings("unchecked")
    public T getMapper() {
        T mapper;

        if (factory != null) {
            mapper = (T) new ObjectMapper(factory);
        } else if (mapperSupplier != null) {
            mapper = mapperSupplier.get();
        } else {
            throw new IllegalStateException("No mapper source defined");
        }
        if (configurator != null) {
            configurator.accept(mapper);
        }
        features.forEach(f -> enable(mapper, f));
        mapper.setDefaultTyping(StdTypeResolverBuilder.noTypeInfoBuilder());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new AfterburnerModule());
        if (serialiazers.size() > 0) {
            SimpleModule wrapperModule = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "WrapperModule"));
            serialiazers.forEach(wrapperModule::addSerializer);
            mapper.registerModule(wrapperModule);
        }
        modules.forEach(mapper::registerModule);
        return mapper;
    }
    
    public JacksonBuilder<T> feature(Enum<?> e) {
        features.add(e);
        return this;
    }

    public JacksonBuilder<T> module(Module m) {
        modules.add(m);
        return this;
    }

    private void enable(T mapper, Enum<?> f) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Enum<?>> eclass = (Class<? extends Enum<?>>) f.getClass();
            Method m = mapper.getClass().getMethod("enable", eclass);
            m.invoke(mapper, f);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new IllegalStateException("Unsuable feature " + f.name() + ": " + Helpers.resolveThrowableException(ex), ex);
        }
    }

}
