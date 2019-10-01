package loghub.jmx;

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
    
    default public String[] getProcessorsFailures() {
        return loghub.Stats.getErrors().stream()
                        .map(i -> Helpers.resolveThrowableException((Throwable)i))
                        .toArray(String[]::new)
                        ;
    }

    default public String[] getDecodersFailures() {
        return loghub.Stats.getDecodeErrors().stream()
                        .map(i -> Helpers.resolveThrowableException((Throwable)i))
                        .toArray(String[]::new)
                        ;
    }

    default public String[] getUnhandledExceptions() {
        return loghub.Stats.getExceptions().stream()
                        .map( i -> {
                            StringBuffer exceptionDetails = new StringBuffer();
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

    default public String[] getBlockingMessage() {
        return loghub.Stats.getBlockedError().stream()
                        .toArray(String[]::new)
                        ;
    }

    default public String[] getSendersFailures() {
        return loghub.Stats.getSenderError().stream()
                        .toArray(String[]::new)
                        ;
    }

    default public String[] getReceiversFailures() {
        return loghub.Stats.getReceiverError().stream()
                        .toArray(String[]::new)
                        ;
    }


    public class Implementation extends StandardMBean implements ExceptionsMBean {

        public final static ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Exceptions");
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        public Implementation()
                        throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(ExceptionsMBean.class);
        }

    }


}
