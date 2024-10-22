package loghub.metrics;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import loghub.Helpers;

@MXBean
public interface ExceptionsMBean {

    default String[] getProcessorsFailures() {
        return loghub.metrics.Stats.getErrors().stream()
                        .map(i -> Helpers.resolveThrowableException((Throwable)i))
                        .toArray(String[]::new)
                        ;
    }

    default String[] getDecodersFailures() {
        return Stats.getDecodeErrors().toArray(String[]::new);
    }

    default String[] getUnhandledExceptions() {
        return loghub.metrics.Stats.getExceptions().stream()
                        .map( i -> {
                            StringBuilder exceptionDetails = new StringBuilder();
                            String exceptionMessage = Helpers.resolveThrowableException(i);
                            exceptionDetails.append(exceptionMessage);
                            StackTraceElement[] stack = (i.getCause() != null ? i.getCause() : i).getStackTrace();
                            if (stack.length > 0) {
                                exceptionDetails.append(String.format(" at %s.%s line %d", stack[0].getClassName(), stack[0].getMethodName(), stack[0].getLineNumber()));
                            }
                            return exceptionDetails.toString();
                        })
                        .toArray(String[]::new)
                        ;
    }

    default String[] getSendersFailures() {
        return Stats.getSenderError().toArray(String[]::new);
    }

    default String[] getReceiversFailures() {
        return Stats.getReceiverError().toArray(String[]::new);
    }


    class Implementation extends StandardMBean implements ExceptionsMBean {

        public static final ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Exceptions");
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        public Implementation()
                        throws NotCompliantMBeanException {
            super(ExceptionsMBean.class);
        }

    }

}
