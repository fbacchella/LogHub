package loghub.metrics;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import loghub.Helpers;

@MXBean
public interface ExceptionsMBean {

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default String[] getProcessorsFailures() {
        return loghub.metrics.Stats.getErrors().stream()
                        .map(i -> Helpers.resolveThrowableException(i.payload()))
                        .toArray(String[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default String[] getDecodersFailures() {
        return Stats.getDecodeErrors().toArray(String[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default String[] getUnhandledExceptions() {
        return loghub.metrics.Stats.getExceptions().stream()
                        .map(i -> {
                            StringBuilder exceptionDetails = new StringBuilder();
                            String exceptionMessage = Helpers.resolveThrowableException(i);
                            exceptionDetails.append(exceptionMessage);
                            StackTraceElement[] stack = (i.getCause() != null ? i.getCause() : i).getStackTrace();
                            if (stack.length > 0) {
                                exceptionDetails.append(String.format(" at %s.%s line %d", stack[0].getClassName(), stack[0].getMethodName(), stack[0].getLineNumber()));
                            }
                            return exceptionDetails.toString();
                        })
                        .toArray(String[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default String[] getSendersFailures() {
        return Stats.getSenderError().toArray(String[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default String[] getReceiversFailures() {
        return Stats.getReceiverError().toArray(String[]::new);
    }

    class Implementation extends DocumentedMBean implements ExceptionsMBean {

        public static final ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Exceptions");
            } catch (MalformedObjectNameException e) {
                throw new IllegalStateException(e.getMessage());
            }
        }

        public Implementation()
                        throws NotCompliantMBeanException {
            super(ExceptionsMBean.class);
        }

    }

}
