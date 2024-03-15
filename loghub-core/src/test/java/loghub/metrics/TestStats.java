package loghub.metrics;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStats {

    @Test
    public void test1() throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
        Stats.reset();
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new NullPointerException();
        Stats.newUnhandledException(e);
        assertEquals(String.format("NullPointerException at loghub.metrics.TestStats.test1 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
        assertEquals(1, Stats.getExceptionsCount());
    }

    @Test
    public void test2() throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
        Stats.reset();
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new RuntimeException("some message");
        Stats.newUnhandledException(e);
        assertEquals(String.format("some message at loghub.metrics.TestStats.test2 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
        assertEquals(1, Stats.getExceptionsCount());
    }

}
