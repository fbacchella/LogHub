package loghub.jackson;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.FormatFeature;
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

import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class JacksonBuilder<T extends ObjectMapper> {

    public static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { /* empty */ };
    public static final TypeReference<HashMap<?, ?>> HAHSMAPREF = new TypeReference<HashMap<?, ?>>() { /* empty */ };

    @Setter
    JsonFactory factory = null;
    @Setter
    private FormatSchema schema = null;
    @Setter @Singular
    Set<Module> modules = new HashSet<>();
    @Setter @Singular
    Set<FormatFeature> features = new HashSet<>();
    @Setter @Singular
    Set<JsonSerializer<?>> serialiazers  = new HashSet<>();
    @Setter
    Supplier<T> mapperSupplier;
    @Setter
    TypeReference<?> typeReference = null;
    @Setter
    Consumer<T> configurator = null;

    public static <T extends ObjectMapper> JacksonBuilder<T> get(Class<T> clazz) {
        return new JacksonBuilder<T>();
    }

    public static JacksonBuilder<ObjectMapper> get() {
        return new JacksonBuilder<ObjectMapper>();
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
        ObjectWriter writer = getMapper().writerFor(typeReference != null ? typeReference: HAHSMAPREF);
        if (schema != null) {
            writer = writer.with(schema);
        }
        return writer;
    }

    @SuppressWarnings("unchecked")
    public T getMapper() {
        T mapper = null;

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

}
