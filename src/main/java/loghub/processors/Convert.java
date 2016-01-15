package loghub.processors;

import java.lang.reflect.InvocationTargetException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
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
public class Convert extends FieldsProcessor {

    private static final Logger logger = LogManager.getLogger();

    private String className = "java.lang.String";
    private Class<?> clazz;

    @Override
    public void processMessage(Event event, String field, String destination) {
        try {
            Object o = BeansManager.ConstructFromString(clazz, event.get(field).toString());
            event.put(destination, o);
        } catch (InvocationTargetException e) {
        }
    }

    @Override
    public String getName() {
        return null;
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
