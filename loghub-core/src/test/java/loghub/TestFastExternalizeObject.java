package loghub;

import java.io.IOException;
import java.io.ObjectOutputStream;
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
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.types.Dn;
import loghub.types.MacAddress;

public class TestFastExternalizeObject {

    private final EventsFactory factory = new EventsFactory();

    /**
     * A canary object that detect unexpected serialisation
     */
    public class UnserializableObject implements Serializable {

        private static final long serialVersionUID = 1L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new IOException("Serialization not allowed for this object.");
        }
    }
    private final UnserializableObject CANARY = new UnserializableObject();

    public static class CloneableObject implements Cloneable {
        @Override
        public Object clone() throws CloneNotSupportedException {
            return this;
        }
    }

    private void checkIdentity(Object o) throws IOException, ClassNotFoundException {
        Assert.assertSame(o, FastExternalizeObject.duplicate(o));
    }

    private void checkEquality(Object o) throws IOException, ClassNotFoundException {
        Object duplicated = FastExternalizeObject.duplicate(o);
        Assert.assertNotSame(o, duplicated);
        Assert.assertEquals(o, duplicated);
    }

    @Test
    public void testIdentity() throws IOException, ClassNotFoundException {
        checkIdentity(1l);
        checkIdentity(1);
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
        checkIdentity(new CloneableObject());
        // Avoid static compilation of a constant
        checkIdentity(new StringBuffer("Log").append("Hub").toString());
        checkEquality(new Date());
        checkEquality(1.0f);
        checkEquality(1.0);
        Object[] array = new Object[]{true, false, 1.0f, 1.0};
        Assert.assertArrayEquals(array, FastExternalizeObject.duplicate(array));
        int[] intArray = new int[]{1, 2, 3};
        Assert.assertArrayEquals(intArray, FastExternalizeObject.duplicate(intArray));
        byte[] byteArray = new byte[]{1, 2, 3};
        Assert.assertArrayEquals(byteArray, FastExternalizeObject.duplicate(byteArray));
        double[] doubleArray = new double[]{1, 2, 3};
        Assert.assertArrayEquals(doubleArray, FastExternalizeObject.duplicate(doubleArray), 1e-5);
        Map<DayOfWeek, Integer> daysMapping = Arrays.stream(DayOfWeek.values()).collect(
                Collectors.toMap(
                        d -> d, DayOfWeek::getValue,
                        (a, b) -> b,
                        () -> new EnumMap<>(DayOfWeek.class)));
        // Ensure that the type is kept
        EnumMap<DayOfWeek, Integer> duplicatedEnumMap = (EnumMap<DayOfWeek, Integer>) FastExternalizeObject.duplicate(daysMapping);
        Assert.assertEquals(daysMapping, duplicatedEnumMap);
        Map<?, ?> map = Map.of("a", true, 'b', false);
        Assert.assertEquals(map, FastExternalizeObject.duplicate(map));
        List l1 = new ArrayList<>(List.of(1, 2, 3));
        Assert.assertEquals(l1, FastExternalizeObject.duplicate(l1));
        List l2 = new LinkedList<>(l1);
        Assert.assertEquals(l2, FastExternalizeObject.duplicate(l2));
    }

    @Test
    public void fails() {
        Assert.assertThrows(IOException.class, () -> FastExternalizeObject.duplicate(Map.of("canary", CANARY)));
        Event ev = factory.newEvent();
        ev.putMeta("canary", Map.of("canary", CANARY));
        loghub.ProcessorException pe = Assert.assertThrows(loghub.ProcessorException.class, ev::duplicate);
        Assert.assertSame(IOException.class, pe.getCause().getClass());
    }

}
