package loghub;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BeanChecks {

    private static Logger logger;

    private static class BeanInfo {
        private final String beanName;
        private final Class<? extends Object> beanType;
        private BeanInfo(String beanName, Class<? extends Object> beanType) {
            this.beanName = beanName;
            this.beanType = beanType;
        }
        private static BeanInfo build(String beanName, Class<? extends Object> beanType) {
            return new BeanInfo(beanName, beanType);
        }
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }


    private void beansCheck(String className, BeanInfo... beans) throws IntrospectionException, ClassNotFoundException {
        Class<? extends Object> testedClass = getClass().getClassLoader().loadClass(className);
        for (BeanInfo bi: beans) {
            PropertyDescriptor bean = new PropertyDescriptor(bi.beanName, testedClass);
            logger.debug("Bean {} for {} is {}", bi.beanName, className, bean);
            Assert.assertEquals(bi.beanType, bean.getPropertyType());
            Assert.assertNotNull(bean.getWriteMethod());
            Assert.assertNotNull(bean.getReadMethod());
        }
    }

    @Test
    public void test_loghub_Receiver() throws ClassNotFoundException, IntrospectionException {
        beansCheck("loghub.Receiver"
                ,BeanInfo.build("decoder", Decoder.class)
                ,BeanInfo.build("useJwt", Boolean.TYPE)
                ,BeanInfo.build("user", String.class)
                ,BeanInfo.build("password", String.class)
                ,BeanInfo.build("jaasName", String.class)
                ,BeanInfo.build("withSSL", Boolean.TYPE)
                ,BeanInfo.build("SSLClientAuthentication", String.class)
                ,BeanInfo.build("SSLKeyAlias", String.class)
                );
    }

    @Test
    public void test_loghub_receivers_Http() throws ClassNotFoundException, IntrospectionException {
        beansCheck("loghub.receivers.Http"
                ,BeanInfo.build("Decoders", Object.class)
                );
    }

    @Test
    public void test_loghub_receivers_ZMQ() throws ClassNotFoundException, IntrospectionException {
        beansCheck("loghub.receivers.ZMQ"
                ,BeanInfo.build("method", String.class)
                ,BeanInfo.build("listen", String.class)
                ,BeanInfo.build("type", String.class)
                ,BeanInfo.build("hwm", Integer.TYPE)
                );
    }

}
