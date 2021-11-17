package loghub;

import static java.util.Locale.ENGLISH;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;

public class BeanChecks {

    private static Logger logger;

    public static final Class<? extends String[]> LSTRING = new String[] {}.getClass();

    public static class BeanInfo {
        private final String beanName;
        private final Class<? extends Object> beanType;
        private BeanInfo(String beanName, Class<? extends Object> beanType) {
            this.beanName = beanName;
            this.beanType = beanType;
        }
        public static BeanInfo build(String beanName, Class<? extends Object> beanType) {
            return new BeanInfo(beanName, beanType);
        }
        private PropertyDescriptor getPropertyDescriptor(Class<?> testedClass) throws IntrospectionException {
            return new PropertyDescriptor(beanName, testedClass, null, "set" + beanName.substring(0, 1).toUpperCase(ENGLISH) + beanName.substring(1));

        }
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    public static void beansCheck(Logger callerLogger, String className, BeanInfo... beans) throws ReflectiveOperationException, IntrospectionException {
        Class<? extends Object> testedClass = BeanChecks.class.getClassLoader().loadClass(className);
        // Tests that the abstract builder can resolver the Builder class if provided
        AbstractBuilder.resolve(testedClass);
        BuilderClass bca = testedClass.getAnnotation(BuilderClass.class);
        if (bca != null) {
            testedClass = bca.value();
        } else {
            Constructor<?> init = testedClass.getConstructor();
            int mod = init.getModifiers();
            Assert.assertTrue(Modifier.isPublic(mod));
        }
        for (BeanInfo bi: beans) {
            PropertyDescriptor bean;
            bean = bi.getPropertyDescriptor(testedClass);
            callerLogger.debug("Bean '{}' for {} is {}", bi.beanName, className, bean);
            Assert.assertEquals(bi.beanType, bean.getPropertyType());
            Assert.assertNotNull(bean.getWriteMethod());
        }
    }

}
