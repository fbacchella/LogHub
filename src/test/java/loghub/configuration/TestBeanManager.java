package loghub.configuration;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import loghub.Expression;
import loghub.ProcessorException;
import lombok.Getter;
import lombok.Setter;

public class TestBeanManager {

    public static class BeanContener {
        @Getter @Setter
        private int integer;
        @Getter @Setter
        private boolean bool;
        @Getter @Setter
        private double doublefloat;
        @Getter @Setter
        private float simplefloat;
        @Getter @Setter
        private byte octet;
        @Getter @Setter
        private long i64;
        @Getter @Setter
        private short i16;
        @Getter @Setter
        private char character;
        @Getter @Setter
        private TimeUnit enumeration = null;
        @Getter @Setter
        private BeanContener[] bc;
        @Getter @Setter
        private Expression ex;
    }

    private final BeansManager beansManager = new BeansManager();

    @Test
    public void testBoolean() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "bool", true);
        Assert.assertTrue(c.isBool());
        beansManager.beanSetter(c, "bool", Boolean.FALSE);
        Assert.assertFalse(c.isBool());
        beansManager.beanSetter(c, "bool", Boolean.FALSE.toString());
        Assert.assertFalse(c.isBool());
    }

    @Test
    public void testDouble() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "doublefloat", Math.E);
        Assert.assertEquals(Math.E, c.getDoublefloat(), 0);
        beansManager.beanSetter(c, "doublefloat", Math.PI);
        Assert.assertEquals(Math.PI, c.getDoublefloat(), 0);
        beansManager.beanSetter(c, "doublefloat", "1.3333");
        Assert.assertEquals((float)1.3333, 1e-4, c.getDoublefloat());
    }

    @Test
    public void testFloat() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "simplefloat", (float) Math.E);
        Assert.assertEquals((float)Math.E, c.getSimplefloat(), 0);
        beansManager.beanSetter(c, "simplefloat", (float) Math.PI);
        Assert.assertEquals((float)Math.PI, c.getSimplefloat(), 0);
        beansManager.beanSetter(c, "simplefloat", "1.3333");
        Assert.assertEquals((float)1.3333, 1e-4, c.getSimplefloat());
    }

    @Test
    public void testByte() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "octet", (byte)4);
        Assert.assertEquals((byte)4, c.getOctet());
        beansManager.beanSetter(c, "octet", (byte) 8);
        Assert.assertEquals((byte)8, c.getOctet());
        beansManager.beanSetter(c, "octet", "16");
        Assert.assertEquals((byte)16, c.getOctet());
    }

    @Test
    public void testShort() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "i16", (short)4);
        Assert.assertEquals((short)4, c.getI16());
        beansManager.beanSetter(c, "i16", (short) 8);
        Assert.assertEquals((short)8, c.getI16());
        beansManager.beanSetter(c, "i16", "16");
        Assert.assertEquals((short)16, c.getI16());
    }

    @Test
    public void testInteger() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "integer", 4);
        Assert.assertEquals(4, c.getInteger());
        beansManager.beanSetter(c, "integer", 8);
        Assert.assertEquals(8, c.getInteger());
        beansManager.beanSetter(c, "integer", "16");
        Assert.assertEquals(16, c.getInteger());
    }

    @Test
    public void testLong() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "i64", 4L);
        Assert.assertEquals(4L, c.getI64());
        beansManager.beanSetter(c, "i64", 8L);
        Assert.assertEquals(8L, c.getI64());
        beansManager.beanSetter(c, "i64", "16");
        Assert.assertEquals(16L, c.getI64());
    }

    @Test
    public void testCharacter() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "character", 'a');
        Assert.assertEquals('a', c.getCharacter());
        beansManager.beanSetter(c, "character", 'b');
        Assert.assertEquals('b', c.getCharacter());
        beansManager.beanSetter(c, "character", "c");
        Assert.assertEquals('c', c.getCharacter());
    }

    @Test
    public void testEnum() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        beansManager.beanSetter(c, "enumeration", TimeUnit.DAYS);
        Assert.assertEquals(TimeUnit.DAYS, c.getEnumeration());
        beansManager.beanSetter(c, "enumeration", TimeUnit.HOURS.name());
        Assert.assertEquals(TimeUnit.HOURS, c.getEnumeration());
        beansManager.beanSetter(c, "enumeration", TimeUnit.HOURS.name().toLowerCase());
        Assert.assertEquals(TimeUnit.HOURS, c.getEnumeration());
        InvocationTargetException ex = Assert.assertThrows(InvocationTargetException.class, () -> beansManager.beanSetter(c, "enumeration", "badenum"));
        Assert.assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
        Assert.assertEquals("Not matching value badenum", ex.getCause().getMessage());
    }

    @Test
    public void testIntegerArray() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeanContener[] integers = new BeanContener[]{new BeanContener(), new BeanContener()};
        beansManager.beanSetter(c, "bc", integers);
        Assert.assertArrayEquals(integers, c.getBc());
    }

    @Test
    public void testExpression() throws InvocationTargetException, IntrospectionException, ProcessorException {
        BeanContener c = new BeanContener();
        Stream.of((byte) 1, (short) 1, 1, 1L, Math.PI, 1.0f, "a", 'a', this).forEach(o -> {
            try {
                beansManager.beanSetter(c, "ex", o);
                Assert.assertEquals(o, c.getEx().eval(null));
            } catch (InvocationTargetException | IntrospectionException | ProcessorException ex) {
                throw new UndeclaredThrowableException(ex);
            }
        });
    }

}
