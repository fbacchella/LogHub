package loghub.cloners;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.types.Dn;
import loghub.types.MacAddress;

public class TestDeepCloner {

    private final EventsFactory factory = new EventsFactory();

    /**
     * A canary object that detects unexpected serialization
     */
    public static class UnserializableObject implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new IOException("Serialization not allowed for this object.");
        }
    }
    private static final UnserializableObject CANARY = new UnserializableObject();

    private void checkIdentity(Object o) throws NotClonableException {
        Assert.assertSame(o, DeepCloner.clone(o));
    }

    @Test
    public void testIdentity() throws NotClonableException {
        checkIdentity(new MacAddress("01:02:03:04:05:06"));
        checkIdentity(ConnectionContext.EMPTY);
        checkIdentity(new Dn("CN=test, ou=loghub"));
        checkIdentity(new MacAddress("01:02:03:04:05:06"));
    }

    @Test
    public void failsEventDuplicate() {
        Event ev = factory.newEvent();
        ev.putMeta("canary", Map.of("canary", CANARY));
        NotClonableException pe = Assert.assertThrows(NotClonableException.class, ev::duplicate);
        Assert.assertSame(IOException.class, pe.getCause().getClass());
    }

    @Test
    public void testEvent() throws NotClonableException {
        Event ev = factory.newEvent();
        ev.put("message", "message");
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        Assert.assertEquals(Map.copyOf(ev), Map.copyOf(DeepCloner.clone(ev)));
        Event wrapped = ev.wrap(VariablePath.of("a"));
        Assert.assertEquals(Map.copyOf(wrapped), Map.copyOf(DeepCloner.clone(wrapped)));
    }

    @Test
    public void testEventDuplicate() throws NotClonableException {
        Event ev = factory.newTestEvent();
        ev.put("message", "message");
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        ev.putMeta("meta", "meta");
        ev.setTimestamp(Instant.ofEpochMilli(0));
        Event duplicate = ev.duplicate();
        Assert.assertEquals(Map.copyOf(ev), Map.copyOf(duplicate));
        Assert.assertEquals(0, duplicate.getTimestamp().getTime());
        Assert.assertEquals("meta", duplicate.getMeta("meta"));
        Assert.assertTrue(duplicate.isTest());
    }

}
