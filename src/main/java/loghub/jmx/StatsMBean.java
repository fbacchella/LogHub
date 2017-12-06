package loghub.jmx;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

@MXBean
public interface StatsMBean {

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
        return loghub.Stats.getErrors().stream()
                .map( i-> (Throwable) (i.getCause() != null ? i.getCause() :  i))
                .map( i -> i.getClass().getSimpleName() + ":" + i.getMessage())
                .toArray(String[]::new)
                ;
    }

    default public String[] getDecodErrors() {
        return loghub.Stats.getDecodeErrors().stream()
                .map( i-> (Throwable) (i.getCause() != null ? i.getCause() :  i))
                .map( i -> i.getClass().getSimpleName() + ":" + i.getMessage())
                .toArray(String[]::new)
                ;
    }

    default public String[] getExceptions() {
        return loghub.Stats.getExceptions().stream()
                .map( i-> (Throwable) (i.getCause() != null ? i.getCause() :  i))
                .map( i -> {
                    StringBuffer exceptionDetails = new StringBuffer();
                    String exceptionMessage = i.getMessage();
                    if (exceptionMessage == null) {
                        exceptionMessage = i.getClass().getSimpleName();
                    }
                    exceptionDetails.append(exceptionMessage);
                    StackTraceElement[] stack = i.getStackTrace();
                    if (stack.length > 0) {
                        exceptionDetails.append(String.format(" at %s.%s line %d", stack[0].getClassName(), stack[0].getMethodName(), stack[0].getLineNumber()));
                    }
                    return exceptionDetails.toString();
                })
                .toArray(String[]::new)
                ;
    }

    public class Implementation extends StandardMBean implements StatsMBean {

        public final static ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "stats");
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        public Implementation()
                throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(StatsMBean.class);
        }

    }

}
