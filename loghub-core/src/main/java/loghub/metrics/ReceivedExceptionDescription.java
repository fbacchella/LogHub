package loghub.metrics;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import loghub.Helpers;
import loghub.receivers.Receiver;

record ReceivedExceptionDescription(String receiver, String message) {

    ReceivedExceptionDescription(Receiver<?, ?> r, String msg) {
        this(r.getReceiverName(), msg);
    }

    ReceivedExceptionDescription(Receiver<?, ?> r, Throwable ex) {
        this(r.getReceiverName(), Helpers.resolveThrowableException(ex));
    }

    public CompositeData toCompositeData() {
        try {
            return new CompositeDataSupport(TYPE,
                    itemNames,
                    new Object[]{ receiver, message}
            );
        } catch (OpenDataException e) {
            return null;
        }
    }

    private static final CompositeType TYPE;
    private static final String[] itemNames = { "receiver", "message" };
    static {
        String[] itemDescriptions = { "The receiver name", "Root cause message" };
        OpenType<?>[] itemTypes = { SimpleType.STRING, SimpleType.STRING};
        try {
            TYPE = new CompositeType("Received exceptions", "Exception when processings received messages", itemNames, itemDescriptions, itemTypes);
        } catch (OpenDataException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
