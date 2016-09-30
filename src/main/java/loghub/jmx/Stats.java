package loghub.jmx;

import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import loghub.ProcessorException;

@MXBean
@Implementation(loghub.jmx.Stats.StatsImpl.class)
public interface Stats {
    public final static String NAME = "loghub:type=stats";

    default public long getReceived() {
        return loghub.Stats.received.get();
    }

    default public long getDropped() {
        return loghub.Stats.dropped.get();
    }

    default public long getSent() {
        return loghub.Stats.sent.get();
    }

    default public long getFailed() {
        return loghub.Stats.failed.get();
    }

    default public String[] getErrors() {
        List<ProcessorException> errors = loghub.Stats.getErrors();
        return errors.stream()
                .map( i-> (Throwable) (i.getCause() != null ? i.getCause() :  i))
                .map( i -> i.getMessage())
                .toArray(String[]::new)
                ;
    }

    default public String[] getExceptions() {
        List<Exception> errors = loghub.Stats.getExceptions();
        return errors.stream()
                .map( i-> (Throwable) (i.getCause() != null ? i.getCause() :  i))
                .map( i -> {
                    StringBuffer exceptionMessage = new StringBuffer();
                    StackTraceElement[] stack = i.getStackTrace();
                    if ( i instanceof NullPointerException) {
                        exceptionMessage.append("NPE");
                    } else {
                        i.getMessage();
                    }
                    if (stack.length > 0) {
                        exceptionMessage.append(String.format(" at %s.%s line %d", stack[0].getClassName(), stack[0].getMethodName(), stack[0].getLineNumber()));
                    }
                    return exceptionMessage.toString();
                })
                .toArray(String[]::new)
                ;
    }

    public class StatsImpl extends BeanImplementation implements Stats {
        public StatsImpl()
                throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(Stats.class);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

}
