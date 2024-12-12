package loghub.metrics;

import java.lang.reflect.Method;
import java.util.Optional;

import javax.management.MBeanAttributeInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class DocumentedMBean extends StandardMBean {

    protected DocumentedMBean(Class<?> mbeanInterface) throws NotCompliantMBeanException {
        super(mbeanInterface);
    }

    @Override
    protected String getDescription(MBeanAttributeInfo info) {
        String description = null;
        try {
            Method method = getMBeanInterface().getMethod("get" + info.getName());
            if (method.isAnnotationPresent(Description.class)) {
                description = method.getAnnotation(Description.class).value();
            }
        } catch (NoSuchMethodException e) {
            // Ignore exception
        }
        return Optional.ofNullable(description).orElse(super.getDescription(info));
    }

}
