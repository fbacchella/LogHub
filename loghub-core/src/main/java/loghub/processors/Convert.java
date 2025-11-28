package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Function;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Setter;

/**
 * A processor that take a String field and transform it to any object that can
 * take a String as a constructor.
 * It uses the custom class loader.
 *
 * @author Fabrice Bacchella
 *
 */
@FieldsProcessor.ProcessNullField
@BuilderClass(Convert.Builder.class)
public class Convert extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Convert> {
        private String className = "java.lang.String";
        private String charset = null;
        private ByteOrder byteOrder = ByteOrder.nativeOrder();
        private String encoding = null;
        private ClassLoader classLoader = Convert.class.getClassLoader();
        public Convert build() {
            return new Convert(this);
        }
    }
    public static Convert.Builder getBuilder() {
        return new Convert.Builder();
    }

    private final Charset charset;
    private final Class<?> clazz;
    private final ByteOrder byteOrder;
    private final Function<String, byte[]> decoder;

    private Convert(Builder builder) {
        super(builder);
        charset = Optional.ofNullable(builder.charset).map(Charset::forName).orElse(StandardCharsets.UTF_8);
        byteOrder = builder.byteOrder;
        String className = builder.className;
        try {
            clazz = builder.classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        if (builder.encoding != null) {
            decoder = resolveDecoder(builder.encoding);
        } else {
            decoder = null;
        }
    }

    private Function<String,byte[]> resolveDecoder(String encoding) {
        switch (encoding) {
        case "BASE64": {
            Base64.Decoder b64decoder = Base64.getDecoder();
            return b64decoder::decode;
        }
        case "BASE64MIME": {
            Base64.Decoder b64decoder = Base64.getMimeDecoder();
            return b64decoder::decode;
        }
        case "BASE64URL": {
            Base64.Decoder b64decoder = Base64.getUrlDecoder();
            return b64decoder::decode;
        }
        default: {
            throw new IllegalArgumentException(String.format("Unsupported text decoder \"%s\"", encoding));
        }
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            if (decoder != null && value instanceof String) {
                value = decoder.apply((String) value);
            }
            return Expression.convertObject(clazz, value, charset, byteOrder);
        } catch (InvocationTargetException ex) {
            logger.atDebug()
                  .withThrowable(ex.getCause())
                  .log("Failed to parsed event {}", event);
            throw event.buildException("Unable to parse \"%s\" as a %s: %s".formatted(value, clazz.getName(), Helpers.resolveThrowableException(ex.getCause())));
        }
    }

    @Override
    protected boolean isIterable(Event event, VariablePath vp) {
        // Does not make real sense to convert individual bytes
        Object value = event.getAtPath(vp);
        return ! (value instanceof byte[]) && super.isIterable(event, vp);
    }

}
