package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.BeansManager;
import loghub.events.Event;
import loghub.types.MacAddress;
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

    public static class Builder extends FieldsProcessor.Builder<Convert> {
        @Setter
        private String className = "java.lang.String";
        @Setter
        private String charset = null;
        @Setter
        private String byteOrder = "NATIVE";
        @Setter
        private ClassLoader classLoader = Grok.class.getClassLoader();
        public Convert build() {
            return new Convert(this);
        }
    }
    public static Convert.Builder getBuilder() {
        return new Convert.Builder();
    }

    private final Charset charset;
    private final String className;
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
        className = builder.className;
        try {
            clazz = builder.classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value == null) {
            return null;
        } else if (clazz.isAssignableFrom(value.getClass())) {
            // Nothing to do, just return the value
            return value;
        } else if (value instanceof byte[] && clazz == String.class) {
            return new String((byte[]) value, charset);
        } else if (value instanceof byte[] && clazz == MacAddress.class) {
            return new MacAddress((byte[]) value);
        } else if (value instanceof byte[] && InetAddress.class == clazz) {
            try {
                return InetAddress.getByAddress((byte[]) value);
            } catch (UnknownHostException ex) {
                throw event.buildException("Failed to parse IP address", ex);
            }
        } else if (value instanceof byte[]) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) value);
                buffer.order(byteOrder);
                Object o;
                switch (className) {
                case "java.lang.Character":
                    o = buffer.getChar();
                    break;
                case "java.lang.Byte" :
                    o = buffer.get();
                    break;
                case "java.lang.Short":
                    o = buffer.getShort();
                    break;
                case "java.lang.Integer":
                    o = buffer.getInt();
                    break;
                case "java.lang.Long":
                    o = buffer.getLong();
                    break;
                case "java.lang.Float":
                    o = buffer.getFloat();
                    break;
                case "java.lang.Double":
                    o = buffer.getDouble();
                    break;
                default:
                    logger.debug(() -> "Failed to parsed byte array event " + event);
                    throw event.buildException("Unable to parse field as a " + className);
                }
                return o;
            } catch (BufferUnderflowException ex) {
                logger.debug(() -> "Failed to parsed event " + event, ex);
                throw event.buildException("Unable to parse field as a " + className + ", not enough bytes", ex);
            }
        } else {
            String valueStr = value.toString();
            if (valueStr.isBlank()) {
                return RUNSTATUS.NOSTORE;
            } else {
                try {
                    Object o;
                    switch (className) {
                    case "java.lang.Integer":
                        o = Integer.valueOf(valueStr);
                        break;
                    case "java.lang.Byte" :
                        o = Byte.valueOf(valueStr);
                        break;
                    case "java.lang.Short":
                        o = Short.valueOf(valueStr);
                        break;
                    case "java.lang.Long":
                        o = Long.valueOf(valueStr);
                        break;
                    case "java.lang.Float":
                        o = Float.valueOf(valueStr);
                        break;
                    case "java.lang.Double":
                        o = Double.valueOf(valueStr);
                        break;
                    case "java.lang.Boolean":
                        o = Boolean.valueOf(valueStr);
                        break;
                    case "java.net.InetAddress":
                        o = InetAddress.getByName(valueStr);
                        break;
                    default:
                        o = BeansManager.constructFromString(clazz, valueStr);
                        break;
                    }
                    return o;
                } catch (NumberFormatException | InvocationTargetException | UnknownHostException ex) {
                    logger.debug(() -> "Failed to parsed event " + event, ex);
                    throw event.buildException("Unable to parse \""+ valueStr +"\" as a " + className + ": " + Helpers.resolveThrowableException(ex));
                }
            }
        }
    }

    @Override
    protected boolean isIterable(Event event, VariablePath vp) {
        // Does not make real sense to convert individual bytes
        Object value = event.getAtPath(vp);
        return ! (value instanceof byte[]) && super.isIterable(event, vp);
    }

}
