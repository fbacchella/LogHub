package loghub.processors;

import java.lang.reflect.InvocationTargetException;

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
    private Class<?> clazz;

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value == null) {
            return null;
        } else {
            try {
                String valueStr = value.toString();
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
                default:
                    o = BeansManager.ConstructFromString(clazz, valueStr);
                    break;
                }
                return o;
            } catch (NumberFormatException e) {
                throw event.buildException("unable to convert from string to " + className, e);
            } catch (InvocationTargetException e) {
                throw event.buildException("unable to convert from string to " + className, (Exception)e.getCause());
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
