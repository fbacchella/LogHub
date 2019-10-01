package loghub.jmx;

import static org.junit.Assert.*;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.junit.Test;

public class TestStats {

    @Test
    public void test1() throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
        loghub.Stats.reset();
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new NullPointerException();
        loghub.Stats.newProcessorException(e);
        assertEquals(String.format("NullPointerException at loghub.jmx.TestStats.test1 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
    }

    @Test
    public void test2() throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
        loghub.Stats.reset();
        ExceptionsMBean exceptions = new ExceptionsMBean.Implementation();
        Exception e = new RuntimeException("some message");
        loghub.Stats.newProcessorException(e);
        assertEquals(String.format("some message at loghub.jmx.TestStats.test2 line %d", e.getStackTrace()[0].getLineNumber()), exceptions.getUnhandledExceptions()[0]);
    }

}
