package loghub.jackson;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.cfg.MapperBuilder;
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

    public static <T extends ObjectMapper> JacksonBuilder<T> get(Class<T> clazz) {
        try {
            Method builderMethod = clazz.getMethod("builder");
            @SuppressWarnings("unchecked")
            MapperBuilder<T,?> builder = (MapperBuilder<T, ?>) builderMethod.invoke(null);
            defautConfiguration(builder);
            return new JacksonBuilder<>(builder);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Unusable Jackson mapper " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    public static <T extends ObjectMapper> JacksonBuilder<T> get(Class<T> clazz, JsonFactory factory) {
        try {
            Method builderMethod = clazz.getMethod("builder", factory.getClass());
            @SuppressWarnings("unchecked")
            MapperBuilder<T,?> builder = (MapperBuilder<T, ?>) builderMethod.invoke(null, factory);
            defautConfiguration(builder);
            return new JacksonBuilder<>(builder);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Unusable Jackson mapper " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    public static  void defautConfiguration(MapperBuilder<?, ?> builder) {
        builder.setDefaultTyping(StdTypeResolverBuilder.noTypeInfoBuilder());
        builder.addModule(new JavaTimeModule());
        builder.addModule(new Jdk8Module());
        builder.addModule(new AfterburnerModule());
        builder.enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER);
    }

    public static <T extends ObjectMapper,B extends MapperBuilder<T,B>> JacksonBuilder<T> get(B builder) {
        return new JacksonBuilder<>(builder);
    }

    public static final TypeReference<Object> OBJECTREF = new TypeReference<>() {
        /* empty */
    };

    private final MapperBuilder<T,?> builder;

    @Setter
    private FormatSchema schema = null;
    @Setter @Singular
    Set<JsonSerializer<?>> serializers = new HashSet<>();
    @Setter
    TypeReference<?> typeReference = null;
    @Setter
    Consumer<T> configurator = null;

    private JacksonBuilder(MapperBuilder<T, ?> builder) {
        this.builder = builder;
    }

    public ObjectReader getReader() {
        ObjectReader reader = getMapper().readerFor(typeReference != null ? typeReference : OBJECTREF);
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

    public T getMapper() {
        T mapper;
        mapper = builder.build();
        if (configurator != null) {
            configurator.accept(mapper);
        }
        if (!serializers.isEmpty()) {
            SimpleModule wrapperModule = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "WrapperModule"));
            serializers.forEach(wrapperModule::addSerializer);
            mapper.registerModule(wrapperModule);
        }
        return mapper;
    }
    
    public JacksonBuilder<T> feature(Enum<?> e) {
        try {
            Object o = Array.newInstance(e.getClass(), 1);
            Array.set(o, 0, e);
            Method m = builder.getClass().getMethod("enable", o.getClass());
            m.invoke(builder, o);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new IllegalStateException("Unusable feature " + e.name() + ": " + Helpers.resolveThrowableException(ex), ex);
        }
        return this;
    }

    public JacksonBuilder<T> module(Module m) {
        builder.addModule(m);
        return this;
    }

}
