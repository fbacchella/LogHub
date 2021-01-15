package loghub.configuration;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

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
    }

    @Test
    public void testBoolean() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "bool", true);
        Assert.assertEquals(true, c.isBool());
        BeansManager.beanSetter(c, "bool", Boolean.FALSE);
        Assert.assertEquals(false, c.isBool());
        BeansManager.beanSetter(c, "bool", Boolean.FALSE.toString());
        Assert.assertEquals(false, c.isBool());
    }

    @Test
    public void testDouble() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "doublefloat", Math.E);
        Assert.assertEquals(Math.E, c.getDoublefloat(), 0);
        BeansManager.beanSetter(c, "doublefloat", Double.valueOf(Math.PI));
        Assert.assertEquals(Math.PI, c.getDoublefloat(), 0);
        BeansManager.beanSetter(c, "doublefloat", "1.3333");
        Assert.assertEquals((float)1.3333, c.getDoublefloat(), 1e-4);
    }

    @Test
    public void testFloat() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "simplefloat", (float) Math.E);
        Assert.assertEquals((float)Math.E, c.getSimplefloat(), 0);
        BeansManager.beanSetter(c, "simplefloat", Float.valueOf((float)Math.PI) );
        Assert.assertEquals((float)Math.PI, c.getSimplefloat(), 0);
        BeansManager.beanSetter(c, "simplefloat", "1.3333");
        Assert.assertEquals((float)1.3333, c.getSimplefloat(), 1e-4);
    }

    @Test
    public void testByte() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "octet", (byte)4);
        Assert.assertEquals((byte)4, c.getOctet());
        BeansManager.beanSetter(c, "octet", Byte.valueOf((byte)8));
        Assert.assertEquals((byte)8, c.getOctet());
        BeansManager.beanSetter(c, "octet", "16");
        Assert.assertEquals((byte)16, c.getOctet());
    }

    @Test
    public void testShort() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "i16", (short)4);
        Assert.assertEquals((short)4, c.getI16());
        BeansManager.beanSetter(c, "i16", Short.valueOf((short)8));
        Assert.assertEquals((short)8, c.getI16());
        BeansManager.beanSetter(c, "i16", "16");
        Assert.assertEquals((short)16, c.getI16());
    }

    @Test
    public void testInteger() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "integer", 4);
        Assert.assertEquals(4, c.getInteger());
        BeansManager.beanSetter(c, "integer", Integer.valueOf(8));
        Assert.assertEquals(8, c.getInteger());
        BeansManager.beanSetter(c, "integer", "16");
        Assert.assertEquals(16, c.getInteger());
    }

    @Test
    public void testLong() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "i64", 4L);
        Assert.assertEquals(4L, c.getI64());
        BeansManager.beanSetter(c, "i64", Long.valueOf(8L));
        Assert.assertEquals(8L, c.getI64());
        BeansManager.beanSetter(c, "i64", "16");
        Assert.assertEquals(16L, c.getI64());
    }

    @Test
    public void testCharacter() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "character", 'a');
        Assert.assertEquals('a', c.getCharacter());
        BeansManager.beanSetter(c, "character", Character.valueOf('b'));
        Assert.assertEquals('b', c.getCharacter());
        BeansManager.beanSetter(c, "character", "c");
        Assert.assertEquals('c', c.getCharacter());
    }

    @Test
    public void testEnum() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeansManager.beanSetter(c, "enumeration", TimeUnit.DAYS);
        Assert.assertEquals(TimeUnit.DAYS, c.getEnumeration());
        BeansManager.beanSetter(c, "enumeration", TimeUnit.HOURS.name());
        Assert.assertEquals(TimeUnit.HOURS, c.getEnumeration());
    }

    @Test
    public void testIntegerArray() throws InvocationTargetException, IntrospectionException {
        BeanContener c = new BeanContener();
        BeanContener[] integers = new BeanContener[]{new BeanContener(), new BeanContener()};
        BeansManager.beanSetter(c, "bc", integers);
        Assert.assertArrayEquals(integers, c.getBc());
    }

}
