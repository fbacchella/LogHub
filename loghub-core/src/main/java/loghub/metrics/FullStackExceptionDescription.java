package loghub.metrics;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.VarFormatter;
import loghub.events.Event;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public record FullStackExceptionDescription(String eventJson, CONTEXT context, String contextName, Throwable ex) {

    private static final Logger logger = LogManager.getLogger();

    private enum CONTEXT {
        PIPELINE(List.of("event", "pipeline", "throwable")) {
            @Override
            Map<String, Object> values(FullStackExceptionDescription descr) throws OpenDataException {
                return Map.of("event", descr.eventJson, "pipeline", descr.contextName(), "throwable", descr.toCompositeData(descr.ex));
            }
        },
        RECEIVER(List.of("event", "receiver", "throwable")) {
            @Override
            Map<String, Object> values(FullStackExceptionDescription descr) throws OpenDataException {
                return Map.of("event", descr.eventJson, "receiver", descr.contextName(), "throwable", descr.toCompositeData(descr.ex));
            }
        },
        SENDER(List.of("event", "sender", "throwable")) {
            @Override
            Map<String, Object> values(FullStackExceptionDescription descr) throws OpenDataException {
                return Map.of("event", descr.eventJson, "sender", descr.contextName(), "throwable", descr.toCompositeData(descr.ex));
            }
        },
        ;
        private final CompositeType type;
        CONTEXT(List<String> fields) {
            try {
                String[] itemNames = fields.toArray(new String[0]);
                String[] itemDescriptions = { "Serialized event", "The exception receiver name", "Root cause message" };
                OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING, EXCEPTION_TYPE};

                type = new CompositeType("FullStackCompositeData", "A composite type for MyRecord", itemNames, itemDescriptions, itemTypes);
            } catch (OpenDataException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        abstract Map<String, Object> values(FullStackExceptionDescription descr) throws OpenDataException;
    }

    private static final VarFormatter JSON_FORMATER = new VarFormatter("${%j}");
    private static final Function<Event, String> FORMATER = e -> {
        try {
            return JSON_FORMATER.format(e);
        } catch (RuntimeException ex) {
            logger.atError().withThrowable(ex).log("Unformatable event :" + Helpers.resolveThrowableException(ex));
            return "Unformatable event :" + Helpers.resolveThrowableException(ex);
        }
    };

    FullStackExceptionDescription(Event ev, Throwable ex) {
        this(FORMATER.apply(ev), CONTEXT.PIPELINE, ev.getRunningPipeline(), ex);
    }

    FullStackExceptionDescription(Event ev, Sender sender, Throwable ex) {
        this(FORMATER.apply(ev), CONTEXT.SENDER, sender.getSenderName(), ex);
    }

    FullStackExceptionDescription(Receiver<?, ?> receiver, Throwable ex) {
        this("{}", CONTEXT.RECEIVER, receiver.getReceiverName(), ex);
    }

    CompositeData toCompositeData() {
        try {
            return new CompositeDataSupport(
                    context.type,
                    context.values(this)
            );
        } catch (OpenDataException e) {
            return null;
        }
    }

    public static final String DEPTH_ATTRIBUTE = "depth";
    public static final String CLASSNAME_ATTRIBUTE = "className";
    public static final String METHODNAME_ATTRIBUTE = "methodName";
    public static final String FILENAME_ATTRIBUTE = "fileName";
    public static final String LINENUMBER_ATTRIBUTE = "lineNumber";
    public static final String INDEX_ATTRIBUTE = "index";
    public static final String EXCEPTIONCLASS_ATTRIBUTE = "exceptionClass";
    public static final String MESSAGE_ATTRIBUTE = "message";
    public static final String STACKTRACE_ATTRIBUTE = "stackTrace";
    public static final String CAUSES_ATTRIBUTE = "causes";

    private static final CompositeType STACK_TRACE_ELEMENT_TYPE;
    private static final TabularType STACK_TRACE_TYPE;
    private static final CompositeType EXCEPTION_ROW_TYPE;
    private static final TabularType CAUSE_CHAIN_TYPE;
    private static final CompositeType EXCEPTION_TYPE;

    private static final String[] steItemNames = { DEPTH_ATTRIBUTE, CLASSNAME_ATTRIBUTE, METHODNAME_ATTRIBUTE, FILENAME_ATTRIBUTE, LINENUMBER_ATTRIBUTE };
    private static final String[] exRowNames = { INDEX_ATTRIBUTE, EXCEPTIONCLASS_ATTRIBUTE, MESSAGE_ATTRIBUTE, STACKTRACE_ATTRIBUTE };
    private static final String[] exNames = { EXCEPTIONCLASS_ATTRIBUTE, MESSAGE_ATTRIBUTE, STACKTRACE_ATTRIBUTE, CAUSES_ATTRIBUTE };

    static {
        try {
            // 1) StackTraceElement composite type
            String[] steItemDescriptions = {
                    "Stack depth",
                    "Declaring class name",
                    "Method name",
                    "File name",
                    "Line number"
            };
            OpenType<?>[] steItemTypes = {
                    SimpleType.INTEGER,
                    SimpleType.STRING,
                    SimpleType.STRING,
                    SimpleType.STRING,
                    SimpleType.INTEGER
            };
            STACK_TRACE_ELEMENT_TYPE = new CompositeType(
                    "StackTraceElementType",
                    "Represents one stack trace element",
                    steItemNames,
                    steItemDescriptions,
                    steItemTypes
            );

            // 2) Tabular type for a full stack trace
            STACK_TRACE_TYPE = new TabularType(
                    "StackTraceType",
                    "Represents a full stack trace",
                    STACK_TRACE_ELEMENT_TYPE,
                    new String[]{DEPTH_ATTRIBUTE} // key columns
            );

            // 3) Exception row type (used as rows in the causes table)
            String[] exRowDescriptions = {
                    "Index in cause chain (0 = immediate cause)",
                    "Exception class name",
                    "Exception message",
                    "Stack trace (tabular)"
            };
            OpenType<?>[] exRowTypes = {
                    SimpleType.INTEGER,
                    SimpleType.STRING,
                    SimpleType.STRING,
                    STACK_TRACE_TYPE
            };
            EXCEPTION_ROW_TYPE = new CompositeType(
                    "ExceptionRowType",
                    "Represents one exception in the cause chain",
                    exRowNames,
                    exRowDescriptions,
                    exRowTypes
            );

            // 4) Tabular type for the chain of causes
            CAUSE_CHAIN_TYPE = new TabularType(
                    "CauseChainType",
                    "Represents the chain of causes",
                    EXCEPTION_ROW_TYPE,
                    new String[] { INDEX_ATTRIBUTE }
            );

            // 5) Top-level exception type that includes causes table
            String[] exDescriptions = {
                    "Top-level exception class",
                    "Top-level exception message",
                    "Top-level stack trace",
                    "Cause chain (tabular)"
            };
            OpenType<?>[] exTypes = {
                    SimpleType.STRING,
                    SimpleType.STRING,
                    STACK_TRACE_TYPE,
                    CAUSE_CHAIN_TYPE
            };
            EXCEPTION_TYPE = new CompositeType(
                    "ExceptionType",
                    "Represents an exception with stack trace and cause chain",
                    exNames,
                    exDescriptions,
                    exTypes
            );

        } catch (OpenDataException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Convert a Throwable to a CompositeData including its stack trace and causes.
     *
     * @param t the throwable to convert (must not be null)
     * @return CompositeData representing the throwable
     * @throws OpenDataException on OpenType construction errors
     */
    private CompositeData toCompositeData(Throwable t) throws OpenDataException {
        // top-level stack trace
        TabularDataSupport topStack = buildStackTraceTabular(t.getStackTrace());

        // build causes chain as a TabularData (index = 0 for immediate cause)
        TabularDataSupport causes = new TabularDataSupport(CAUSE_CHAIN_TYPE);
        Throwable cause = t.getCause();
        int idx = 0;
        while (cause != null) {
            TabularDataSupport causeStack = buildStackTraceTabular(cause.getStackTrace());
            Object[] rowValues = new Object[] {
                    idx,
                    cause.getClass().getName(),
                    cause.getMessage(),
                    causeStack
            };
            CompositeData row = new CompositeDataSupport(
                    EXCEPTION_ROW_TYPE,
                    exRowNames,
                    rowValues
            );
            causes.put(row);
            cause = cause.getCause();
            idx++;
        }

        Object[] values = new Object[] {
                t.getClass().getName(),
                t.getMessage(),
                topStack,
                causes
        };

        return new CompositeDataSupport(
                EXCEPTION_TYPE,
                exNames,
                values
        );
    }

    // Helper: build a TabularDataSupport from a StackTraceElement[]
    private TabularDataSupport buildStackTraceTabular(StackTraceElement[] stes) throws OpenDataException {
        TabularDataSupport tab = new TabularDataSupport(STACK_TRACE_TYPE);
        if (stes != null) {
            int i = 0;
            for (StackTraceElement ste : stes) {
                Object[] steValues = new Object[] {
                        i++,
                        ste.getClassName(),
                        ste.getMethodName(),
                        ste.getFileName(),
                        ste.getLineNumber()
                };
                CompositeData steRow = new CompositeDataSupport(
                        STACK_TRACE_ELEMENT_TYPE,
                        steItemNames,
                        steValues
                );
                tab.put(steRow);
            }
        }
        return tab;
    }

}
