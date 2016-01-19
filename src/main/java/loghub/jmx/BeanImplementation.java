package loghub.jmx;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public abstract class BeanImplementation extends StandardMBean {

    protected BeanImplementation(Class<?> mbeanInterface)
            throws NotCompliantMBeanException {
        super(mbeanInterface);
    }

    public abstract String getName();
}
