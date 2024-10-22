package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

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
        private String byteOrder = "NATIVE";
        private ClassLoader classLoader = Grok.class.getClassLoader();
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

    private Convert(Builder builder) {
        super(builder);
        charset = Optional.ofNullable(builder.charset).map(Charset::forName).orElse(StandardCharsets.UTF_8);
        switch (builder.byteOrder) {
        case "BIG_ENDIAN":
            byteOrder = ByteOrder.BIG_ENDIAN;
            break;
        case "LITTLE_ENDIAN":
            byteOrder = ByteOrder.LITTLE_ENDIAN;
            break;
        default:
            byteOrder = ByteOrder.nativeOrder();
        }
        String className = builder.className;
        try {
            clazz = builder.classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        try {
            return Expression.convertObject(clazz, value, charset, byteOrder);
        } catch (BufferUnderflowException ex) {
            throw event.buildException("Unable to parse field as a " + clazz.getName() + ", not enough bytes", ex);
        } catch (UnknownHostException ex) {
            throw event.buildException("\"" + value + "\" not a valid IP address", ex);
        } catch (NumberFormatException ex) {
             throw event.buildException("Unable to parse \""+ value +"\" as a " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex));
        } catch (InvocationTargetException ex) {
            logger.atDebug()
                  .withThrowable(ex.getCause())
                  .log("Failed to parsed event {}", event);
            throw event.buildException("Unable to parse \""+ value +"\" as a " + clazz.getName() + ": " + Helpers.resolveThrowableException(ex));
        }
    }

    @Override
    protected boolean isIterable(Event event, VariablePath vp) {
        // Does not make real sense to convert individual bytes
        Object value = event.getAtPath(vp);
        return ! (value instanceof byte[]) && super.isIterable(event, vp);
    }

}
