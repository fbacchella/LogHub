package loghub.cloners;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
     * A canary object that detect unexpected serialisation
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

    public static class CloneableObject implements Cloneable {
        @Override
        public Object clone() {
            return this;
        }
    }

    public static class NotCloneableObject implements Cloneable {
        @Override
        public Object clone() {
            throw new IllegalStateException("Not clonable");
        }
    }

    public static class NotForkableObject {

    }

    private record RecordTester(int v) {

    }

    private void checkIdentity(Object o) throws NotClonableException {
        Assert.assertSame(o, DeepCloner.clone(o));
    }

    private void checkEquality(Object o) throws NotClonableException {
        Object duplicated = DeepCloner.clone(o);
        Assert.assertNotSame(o, duplicated);
        Assert.assertEquals(o, duplicated);
    }

    @Test
    public void testIdentity() throws IOException, NotClonableException {
        checkIdentity(1L);
        checkIdentity(1);
        checkIdentity((byte)1);
        checkIdentity(true);
        checkIdentity(false);
        checkIdentity('a');
        checkIdentity(ChronoUnit.ERAS);
        checkIdentity(Instant.now());
        checkIdentity(InetAddress.getLoopbackAddress());
        checkIdentity(Inet4Address.getByName("localhost"));
        checkIdentity(Inet6Address.getByName("localhost"));
        checkIdentity(new InetSocketAddress(0));
        checkIdentity(new MacAddress("01:02:03:04:05:06"));
        checkIdentity(ConnectionContext.EMPTY);
        checkIdentity(new Dn("CN=test, ou=loghub"));
        checkIdentity(new MacAddress("01:02:03:04:05:06"));
        checkIdentity(UUID.randomUUID());
        checkIdentity(Map.of());
        checkIdentity(List.of());
        checkIdentity(Collections.emptyMap());
        checkIdentity(Collections.emptyList());
        checkIdentity(new CloneableObject());
        // Avoid static compilation of a constant
        checkIdentity(new StringBuffer("Log").append("Hub").toString());
        checkEquality(new Date());
        checkIdentity(1.0f);
        checkIdentity(1.0);
        checkIdentity(new RecordTester(1));
        Object[] array = new Object[]{true, false, 1.0f, 1.0};
        Assert.assertArrayEquals(array, DeepCloner.clone(array));
        int[] intArray = new int[]{1, 2, 3};
        Assert.assertArrayEquals(intArray, DeepCloner.clone(intArray));
        byte[] byteArray = new byte[]{1, 2, 3};
        Assert.assertArrayEquals(byteArray, DeepCloner.clone(byteArray));
        double[] doubleArray = new double[]{1, 2, 3};
        Assert.assertArrayEquals(doubleArray, DeepCloner.clone(doubleArray), 1e-5);
        Map<DayOfWeek, Integer> daysMapping = Arrays.stream(DayOfWeek.values()).collect(
                Collectors.toMap(
                        d -> d, DayOfWeek::getValue,
                        (a, b) -> b,
                        () -> new EnumMap<>(DayOfWeek.class)));
        // Ensure that the type is kept
        EnumMap<DayOfWeek, Integer> duplicatedEnumMap = (EnumMap<DayOfWeek, Integer>) DeepCloner.clone(daysMapping);
        Assert.assertEquals(daysMapping, duplicatedEnumMap);
        Map<?, ?> map = Map.of("a", true, 'b', false);
        Assert.assertEquals(map, DeepCloner.clone(map));
        List<?> l1 = new ArrayList<>(List.of(1, 2, 3));
        Assert.assertEquals(l1, DeepCloner.clone(l1));
        List<?> l2 = new LinkedList<>(l1);
        Assert.assertEquals(l2, DeepCloner.clone(l2));
        Set<?> s1 = new HashSet<>(l1);
        Assert.assertEquals(s1, DeepCloner.clone(s1));
    }

    @Test
    public void failsEventDuplicate() {
        Event ev = factory.newEvent();
        ev.putMeta("canary", Map.of("canary", CANARY));
        NotClonableException pe = Assert.assertThrows(NotClonableException.class, ev::duplicate);
        Assert.assertSame(IOException.class, pe.getCause().getClass());
    }

    @Test
    public void failsClone() {
        // Using a collection to ensure that the failure is really done at the right class
        checkFails(List.of("canary", CANARY), "loghub.cloners.TestDeepCloner.UnserializableObject");
        checkFails(Map.of("canary", new NotCloneableObject()), "loghub.cloners.TestDeepCloner.NotCloneableObject");
        checkFails(new Object[]{new NotForkableObject()}, "loghub.cloners.TestDeepCloner.NotForkableObject");
    }

    private void checkFails(Object o, String exptectedMessage) {
        String message = Assert.assertThrows(NotClonableException.class, () -> DeepCloner.clone(o)).getMessage();
        Assert.assertEquals(exptectedMessage, message);

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

    @Test
    public void testProp() throws NotClonableException {
        Properties p = new Properties();
        p.put("a", List.of(1));
        Properties pc = DeepCloner.clone(p);
        Assert.assertEquals(p, pc);
        Assert.assertNotSame(p.get("a"), pc.get("a"));
        Assert.assertEquals(p.get("a"), pc.get("a"));
    }

}
