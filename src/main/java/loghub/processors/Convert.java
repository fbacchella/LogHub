package loghub.processors;

import java.lang.reflect.InvocationTargetException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.BeansManager;
import loghub.configuration.Properties;

/**
 * A processor that take a String field and transform it to any object that can
 * take a String as a constructor.
 * 
 * It uses the custom class loader.
 * 
 * @author Fabrice Bacchella
 *
 */
@FieldsProcessor.ProcessNullField
public class Convert extends FieldsProcessor {

    private String className = "java.lang.String";
    private String charset = null;
    private Charset effectiveCharset = StandardCharsets.UTF_8;
    private Class<?> clazz;
    private String byteOrder = null;
    private ByteOrder effectiveByteOrder = ByteOrder.nativeOrder();

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value == null) {
            return null;
        } else if (value instanceof byte[] && "java.lang.String".equals(className)) {
            return new String((byte[]) value, effectiveCharset);
        } else if (value instanceof byte[]) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) value);
                buffer.order(effectiveByteOrder);
                Object o;
                switch(className) {
                case "java.lang.Integer":
                    o = buffer.getInt();
                    break;
                case "java.lang.Byte" :
                    o = buffer.get();
                    break;
                case "java.lang.Short":
                    o = buffer.getShort();
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
            try {
                Object o;
                switch(className) {
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
                default:
                    o = BeansManager.constructFromString(clazz, valueStr);
                    break;
                }
                return o;
            } catch (NumberFormatException e) {
                logger.debug(() -> "Failed to parsed event " + event, e);
                throw event.buildException("Unable to parse \""+ valueStr +"\" as a " + className);
            } catch (InvocationTargetException e) {
                logger.debug(() -> "Failed to parsed event " + event, e);
                throw event.buildException("Unable to parse \""+ valueStr +"\" as a " + className, (Exception)e.getCause());
            }
        }
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            clazz = properties.classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            logger.error("class not found: {}", className);
            return false;
        }
        if (charset != null) {
            effectiveCharset = Charset.forName(charset);
        }
        if (byteOrder != null) {
            switch (byteOrder.toUpperCase(Locale.ENGLISH)) {
                case "BIG_ENDIAN":
                    effectiveByteOrder = ByteOrder.BIG_ENDIAN;
                    break;
                case "LITTLE_ENDIAN":
                    effectiveByteOrder = ByteOrder.LITTLE_ENDIAN;
                    break;
            }
        }
        return super.configure(properties);
    }

    /**
     * @return the field
     */
    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

}
