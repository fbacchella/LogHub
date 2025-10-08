package loghub.metrics;

import java.util.List;
import java.util.Map;

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

public record EventExceptionDescription(String eventJson, CONTEXT context, String contextName, String message) {
    private static final VarFormatter JSON_FORMATER = new VarFormatter("${%j}");

    private enum CONTEXT {
        PIPELINE(List.of("event", "pipeline", "message")) {
            @Override
            Map<String, Object> values(EventExceptionDescription descr) {
                return Map.of("event", descr.eventJson, "pipeline", descr.contextName(), "message", descr.message);
            }
        },
        SENDER(List.of("event", "sender", "message")) {
            @Override
            Map<String, Object> values(EventExceptionDescription descr) {
                return Map.of("event", descr.eventJson, "sender", descr.contextName(), "message", descr.message);
            }
        },
        ;
        private final CompositeType type;
        CONTEXT(List<String> fields) {
            try {
                String[] itemNames = fields.toArray(new String[0]);
                String[] itemDescriptions = { "Serialized event", "Then name of the receiver where the exception occurs", "Root cause message" };
                OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING};
                type = new CompositeType("FullStackCompositeData", "A composite type for MyRecord", itemNames, itemDescriptions, itemTypes);
            } catch (OpenDataException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        abstract Map<String, Object> values(EventExceptionDescription descr);
    }

    EventExceptionDescription(ProcessorException ex) {
        this(JSON_FORMATER.format(ex.getEvent()), CONTEXT.PIPELINE, ex.getEvent().getRunningPipeline(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, Throwable ex) {
        this(JSON_FORMATER.format(event), CONTEXT.SENDER, sender.getSenderName(), Helpers.resolveThrowableException(ex));
    }

    EventExceptionDescription(Event event, Sender sender, String message) {
        this(JSON_FORMATER.format(event), CONTEXT.SENDER, sender.getSenderName(), message);
    }

    EventExceptionDescription(Event event, Sender sender) {
        this(JSON_FORMATER.format(event), CONTEXT.SENDER, sender.getSenderName(), "Generic failure");
    }

    CompositeDataSupport toCompositeData() {
        try {
            return new CompositeDataSupport(
                    context.type,
                    context.values(this)
            );
        } catch (OpenDataException e) {
            return null;
        }
    }

}
