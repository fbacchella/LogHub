package loghub.metrics;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import loghub.VarFormatter;
import loghub.events.Event;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

record FullStackExceptionDescription(String eventJson, String context, String contextKind, Throwable ex) {

    private static final VarFormatter JSON_FORMATER = new VarFormatter("${%j}");

    FullStackExceptionDescription(Event ev, Throwable ex) {
        this(JSON_FORMATER.format(ev), ev.getRunningPipeline(), "Pipeline", ex);
    }

    FullStackExceptionDescription(Event ev, Sender sender, Throwable ex) {
        this(JSON_FORMATER.format(ev), sender.getSenderName(), "Sender", ex);
    }

    FullStackExceptionDescription(Receiver<?, ?> receiver, Throwable ex) {
        this("{}", receiver.getReceiverName(), "Receiver", ex);
    }

    CompositeData toCompositeData() {
        try {
            return new CompositeDataSupport(
                    INSTANCE,
                    itemNames,
                    new Object[]{eventJson, context, contextKind, toCompositeData(ex)}
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

    private static final String[] itemNames = { "event", "receiver", "contextKind", "throwable" };
    private static final String[] itemDescriptions = { "Serialized event", "The exception receiver name", "The kind of receiver", "Root cause message" };
    private static final OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, EXCEPTION_TYPE};

    private static final CompositeType INSTANCE;
    static {
        try {
            INSTANCE = new CompositeType("FullStackCompositeData", "A composite type for MyRecord", itemNames, itemDescriptions, itemTypes);
        } catch (OpenDataException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
