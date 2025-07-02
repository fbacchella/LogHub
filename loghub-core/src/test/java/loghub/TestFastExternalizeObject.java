package loghub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.FastExternalizeObject.FastObjectInputStream;
import loghub.FastExternalizeObject.FastObjectOutputStream;
import loghub.FastExternalizeObject.Immutable;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.types.Dn;
import loghub.types.MacAddress;

public class TestFastExternalizeObject {

    private final EventsFactory factory = new EventsFactory();

    /**
     * A canary object that detect unexpected serialisation
     */
    @Immutable
    public class UnserializableObject implements Serializable {

        private static final long serialVersionUID = 1L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new IOException("Serialization not allowed for this object.");
        }
    }
    private final UnserializableObject CANARY = new UnserializableObject();


    private <T> T duplicate(T inputVal) throws IOException, ClassNotFoundException {
        // FastObjectInputStream
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); FastObjectOutputStream oos = new FastObjectOutputStream(bos)) {
            oos.writeObjectFast(inputVal);
            oos.flush();
            bos.flush();
            try (FastObjectInputStream ois = new FastObjectInputStream(bos.toByteArray(), factory, oos)) {
                return (T) ois.readObjectFast();
            }
        }

    }

    private void checkIdentity(Object o) throws IOException, ClassNotFoundException {
        Assert.assertSame(o, duplicate(o));
    }

    private void checkEquality(Object o) throws IOException, ClassNotFoundException {
        Object duplicated = duplicate(o);
        Assert.assertNotSame(o, duplicated);
        Assert.assertEquals(o, duplicated);
    }

    @Test
    public void testIdentity() throws IOException, ClassNotFoundException, ProcessorException {
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
        checkIdentity(CANARY);
        checkIdentity(new Dn("CN=test, ou=loghub"));
        // Avoid static compilation of a constant
        checkIdentity(new StringBuffer("Log").append("Hub").toString());
        checkEquality(new Date());
        checkEquality(1.0f);
        checkEquality(1.0);
        Object[] array = new Object[]{true, false,1.0f, 1.0, CANARY};
        Assert.assertArrayEquals(array, duplicate(array));
        int[] intArray = new int[]{1, 2, 3};
        Assert.assertArrayEquals(intArray, duplicate(intArray));
        byte[] byteArray = new byte[]{1, 2, 3};
        Assert.assertArrayEquals(byteArray, duplicate(byteArray));
        double[] doubleArray = new double[]{1, 2, 3};
        Assert.assertArrayEquals(doubleArray, duplicate(doubleArray), 1e-5);
        Map<?, ?> map = Map.of("a", true, 'b', false);
        Assert.assertEquals(map, duplicate(map));
        List l1 = new ArrayList<>(List.of(1, 2, 3, CANARY));
        Assert.assertEquals(l1, duplicate(l1));
        List l2 = new LinkedList<>(l1);
        Assert.assertEquals(l2, duplicate(l2));

        Event ev = factory.newEvent();
        ev.put("canary", CANARY);
        Assert.assertEquals(ev.duplicate().get("canary"), ev.get("canary"));
    }

    @Test
    public void fails() {
        Assert.assertThrows(IOException.class, () -> duplicate(Map.of("canary", CANARY)));
        Event ev = factory.newEvent();
        ev.putMeta("canary", Map.of("canary", CANARY));
        loghub.ProcessorException pe = Assert.assertThrows(loghub.ProcessorException.class, ev::duplicate);
        Assert.assertSame(IOException.class, pe.getCause().getClass());
    }

}
