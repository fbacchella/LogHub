package loghub;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;

import loghub.processors.FieldsProcessor;

import static java.util.Locale.ENGLISH;

public class BeanChecks {

    public static final Class<? extends String[]> LSTRING = String[].class;

    public static class BeanInfo {
        private final String beanName;
        private final Class<?> beanType;
        private BeanInfo(String beanName, Class<?> beanType) {
            this.beanName = beanName;
            this.beanType = beanType;
        }
        public static BeanInfo build(String beanName, Class<?> beanType) {
            return new BeanInfo(beanName, beanType);
        }
        private PropertyDescriptor getPropertyDescriptor(Class<?> testedClass) throws IntrospectionException {
            return new PropertyDescriptor(beanName, testedClass, null, "set" + beanName.substring(0, 1).toUpperCase(ENGLISH) + beanName.substring(1));

        }
    }

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    public static void beansCheck(Logger callerLogger, String className, BeanInfo... beans) throws ReflectiveOperationException, IntrospectionException {
        Class<?> componentClass = BeanChecks.class.getClassLoader().loadClass(className);
        // Tests that the abstract builder can resolver the Builder class if provided
        AbstractBuilder.resolve(componentClass);
        BuilderClass bca = componentClass.getAnnotation(BuilderClass.class);
        Class<?> testedClass;
        if (bca != null) {
            testedClass = bca.value();
        } else {
            testedClass = componentClass;
            Constructor<?> init = testedClass.getConstructor();
            int mod = init.getModifiers();
            Assert.assertTrue(Modifier.isPublic(mod));
        }
        for (BeanInfo bi: beans) {
            PropertyDescriptor bean;
            bean = bi.getPropertyDescriptor(testedClass);
            callerLogger.debug("Bean '{}' for {} is {}", bi.beanName, className, bean);
            Assert.assertEquals(bi.beanName, bi.beanType, bean.getPropertyType());
            Assert.assertNotNull(bean.getWriteMethod());
            if ("inPlace".equals(bi.beanName) && componentClass.getAnnotation(FieldsProcessor.InPlace.class) == null) {
                Assert.fail("inPlace argument requires InPlace annotation");
            }
        }
    }

}
