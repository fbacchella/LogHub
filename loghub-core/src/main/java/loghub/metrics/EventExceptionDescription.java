package loghub.metrics;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import loghub.Helpers;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.events.Event;
import loghub.senders.Sender;

public record EventExceptionDescription(String eventJson, String context, String message) {
    private static final VarFormatter JSON_FORMATER = new VarFormatter("${%j}");

    EventExceptionDescription(ProcessorException ex) {
        this(JSON_FORMATER.format(ex.getEvent()), ex.getEvent().getRunningPipeline(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, Throwable ex) {
        this(JSON_FORMATER.format(event), sender.getSenderName(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, String message) {
        this(JSON_FORMATER.format(event), sender.getSenderName(), message);
    }

    EventExceptionDescription(Event event, Sender sender) {
        this(JSON_FORMATER.format(event), sender.getSenderName(), "Generic failure");
    }

    CompositeDataSupport toCompositeData() {
        try {
            return new CompositeDataSupport(
                    INSTANCE,
                    itemNames,
                    new Object[]{eventJson, context, message}
            );
        } catch (OpenDataException e) {
            return null;
        }
    }

    private static final String[] itemNames = { "event", "receiver", "message" };
    public static final CompositeType INSTANCE;
    static {
        try {
            String[] itemDescriptions = { "Serialized event", "Then name of the receiver where the exception occurs", "Root cause message" };
            OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING};
            INSTANCE = new CompositeType("FullStackCompositeData", "A composite type for MyRecord", itemNames, itemDescriptions, itemTypes);
        } catch (OpenDataException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
