package loghub.metrics;

import java.util.Objects;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

@MXBean
public interface ExceptionsMBean {

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default CompositeData[] getProcessorsFailures() {
        return loghub.metrics.Stats.getErrors().stream()
                             .map(EventExceptionDescription::toCompositeData)
                             .filter(Objects::nonNull)
                             .toArray(CompositeData[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default CompositeData[] getDecodersFailures() {
        return loghub.metrics.Stats.getDecodeErrors().stream()
                       .map(ReceivedExceptionDescription::toCompositeData)
                       .filter(Objects::nonNull)
                       .toArray(CompositeData[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default CompositeData[] getUnhandledExceptions() {
        return loghub.metrics.Stats.getUnexpectedExceptions().stream()
                        .map(FullStackExceptionDescription::toCompositeData)
                        .filter(Objects::nonNull)
                        .toArray(CompositeData[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default CompositeData[] getSendersFailures() {
        return Stats.getSenderError()
                    .stream()
                    .map(EventExceptionDescription::toCompositeData)
                    .filter(Objects::nonNull)
                    .toArray(CompositeData[]::new);
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default CompositeData[] getReceiversFailures() {
        return Stats.getReceiverError()
                    .stream()
                    .map(ReceivedExceptionDescription::toCompositeData)
                    .filter(Objects::nonNull)
                    .toArray(CompositeData[]::new);
    }

    class Implementation extends DocumentedMBean implements ExceptionsMBean {
//        private static final CompositeType FULL_STACK_COMPOSITE_DATA;
//        private static final CompositeType STACK_TRACE_ELEMENT_TYPE;
//        private static final TabularType STACK_TRACE_TYPE;
//        private static final CompositeType EXCEPTION_TYPE;
//        private static final CompositeType EXCEPTION_ROW_TYPE;
//        private static final TabularType CAUSE_CHAIN_TYPE;
//        static {
//            try {
//                // 1) StackTraceElement composite type
//                String[] steItemNames = { "className", "methodName", "fileName", "lineNumber" };
//                String[] steItemDescriptions = {
//                        "Declaring class name",
//                        "Method name",
//                        "File name",
//                        "Line number"
//                };
//                OpenType<?>[] steItemTypes = {
//                        SimpleType.STRING,
//                        SimpleType.STRING,
//                        SimpleType.STRING,
//                        SimpleType.INTEGER
//                };
//                STACK_TRACE_ELEMENT_TYPE = new CompositeType(
//                        "StackTraceElementType",
//                        "Represents one stack trace element",
//                        steItemNames,
//                        steItemDescriptions,
//                        steItemTypes
//                );
//
//                // 2) Tabular type for a full stack trace
//                STACK_TRACE_TYPE = new TabularType(
//                        "StackTraceType",
//                        "Represents a full stack trace",
//                        STACK_TRACE_ELEMENT_TYPE,
//                        new String[] { "className", "methodName", "lineNumber" } // key columns
//                );
//
//                // 3) Exception row type (used as rows in the causes table)
//                String[] exRowNames = { "index", "exceptionClass", "message", "stackTrace" };
//                String[] exRowDescriptions = {
//                        "Index in cause chain (0 = immediate cause)",
//                        "Exception class name",
//                        "Exception message",
//                        "Stack trace (tabular)"
//                };
//                OpenType<?>[] exRowTypes = {
//                        SimpleType.INTEGER,
//                        SimpleType.STRING,
//                        SimpleType.STRING,
//                        STACK_TRACE_TYPE
//                };
//                EXCEPTION_ROW_TYPE = new CompositeType(
//                        "ExceptionRowType",
//                        "Represents one exception in the cause chain",
//                        exRowNames,
//                        exRowDescriptions,
//                        exRowTypes
//                );
//
//                // 4) Tabular type for the chain of causes
//                CAUSE_CHAIN_TYPE = new TabularType(
//                        "CauseChainType",
//                        "Represents the chain of causes",
//                        EXCEPTION_ROW_TYPE,
//                        new String[] { "index" } // unique key -> index ensures uniqueness
//                );
//
//                // 5) Top-level exception type that includes causes table
//                String[] exNames = { "exceptionClass", "message", "stackTrace", "causes" };
//                String[] exDescriptions = {
//                        "Top-level exception class",
//                        "Top-level exception message",
//                        "Top-level stack trace",
//                        "Cause chain (tabular)"
//                };
////                OpenType<?>[] exTypes = {
////                        SimpleType.STRING,
////                        SimpleType.STRING,
////                        STACK_TRACE_TYPE,
////                        CAUSE_CHAIN_TYPE
////                };
////                EXCEPTION_TYPE = new CompositeType(
////                        "ExceptionType",
////                        "Represents an exception with stack trace and cause chain",
////                        exNames,
////                        exDescriptions,
////                        exTypes
////                );
//
////                String[] itemNames = { "event", "stack" };
////                String[] itemDescriptions = { "Serialized event", "Full stack" };
////                OpenType<?>[] itemTypes = {SimpleType.STRING, EXCEPTION_TYPE};
////                FULL_STACK_COMPOSITE_DATA = new CompositeType(
////                        "FullStackCompositeData",
////                        "A composite type for MyRecord",
////                        itemNames,
////                        itemDescriptions,
////                        itemTypes
////                );
//            } catch (OpenDataException e) {
//                throw new ExceptionInInitializerError(e);
//            }
//
//        }

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
