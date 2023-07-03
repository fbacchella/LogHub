package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Helpers;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.types.MacAddress;

public class TestConvert {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Convert");
    }

    private void check(String className, Class<?> reference, Object invalue, Object outvalue) throws ProcessorException {
        Convert.Builder builder = Convert.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setClassName(className);
        Convert cv = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(cv.configure(props));

        Event e = factory.newEvent();
        e.put("message",invalue);
        e.process(cv);
        Assert.assertTrue(reference.isAssignableFrom(e.get("message").getClass()));
        Assert.assertTrue(e.get("message").getClass().isAssignableFrom(reference));
        Assert.assertEquals(outvalue, e.get("message"));
    }

    private byte[] generate(Function<ByteBuffer, ByteBuffer> contentSource) {
        return generate(8, contentSource);
    }

    private byte[] generate(int size, Function<ByteBuffer, ByteBuffer> contentSource) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
        buffer.order(ByteOrder.nativeOrder());
        Function<Function<ByteBuffer, ByteBuffer>, byte[]> source = f -> f.apply(buffer.clear()).array();
        return source.apply(contentSource);
    }

    @Test
    public void TestResolution() throws ProcessorException, UnknownHostException {
        check("java.lang.Integer", Integer.class, "38", 38);
        check("java.lang.Byte", Byte.class, "38", (byte) 38);
        check("java.lang.Short", Short.class, "38", (short) 38);
        check("java.lang.Long", Long.class, "38", (long) 38);
        check("java.lang.Double", Double.class, "38", (double) 38);
        check("java.lang.Float", Float.class, "38", (float) 38);
        check("java.net.InetAddress", java.net.Inet4Address.class, "127.0.0.1", InetAddress.getByName("127.0.0.1"));
        check("java.net.InetAddress", java.net.Inet6Address.class, "::1", InetAddress.getByName("::1"));
    }

    @Test
    public void TestResolutionBytes() throws ProcessorException, UnknownHostException {
        check("java.lang.Integer", Integer.class, generate(b -> b.putInt(38)), 38);
        check("java.lang.Byte", Byte.class, generate(b -> b.put((byte)38)), (byte) 38);
        check("java.lang.Short", Short.class, generate(b -> b.putShort((short)38)), (short) 38);
        check("java.lang.Long", Long.class, generate(b -> b.putLong(38)), (long) 38);
        check("java.lang.Double", Double.class, generate(b -> b.putDouble(38)), (double) 38);
        check("java.lang.Float", Float.class, generate(b -> b.putFloat((float)38)), (float) 38);
        check("java.lang.String", String.class, "message with éèœ".getBytes(StandardCharsets.UTF_8), "message with éèœ");
        check("java.net.InetAddress", java.net.Inet4Address.class, InetAddress.getByName("127.0.0.1").getAddress(), InetAddress.getByName("127.0.0.1"));
        check("java.net.InetAddress", java.net.Inet6Address.class, InetAddress.getByName("::1").getAddress(), InetAddress.getByName("::1"));
        check("loghub.types.MacAddress", MacAddress .class, new MacAddress(new byte[]{(byte)0x3D, (byte)0xF2, (byte)0xC9, (byte)0xA6, (byte)0xB3, (byte)0x4F}), new MacAddress(new byte[]{(byte)0x3D, (byte)0xF2, (byte)0xC9, (byte)0xA6, (byte)0xB3, (byte)0x4F}));
    }

    @Test
    public void TestNope() throws ProcessorException, UnknownHostException {
        check("java.lang.Number", Integer.class, 38, 38);
        check("java.net.InetAddress", java.net.Inet4Address.class, InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.1"));
    }

    @Test
    public void TestIterableEtl() throws ProcessorException, IOException {
        String configFile = "pipeline[convert] { (java.lang.Integer)[message] }";
        Properties p =  Configuration.parse(new StringReader(configFile));
        Helpers.parallelStartProcessor(p);
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("message"), List.of("1", "2", "3"));
        Tools.runProcessing(ev, p.namedPipeLine.get("convert"), p);
        Assert.assertEquals(List.of(1, 2, 3), ev.get("message"));
    }

    @Test(expected=loghub.ProcessorException.class)
    public void TestInvalid() throws ProcessorException, UnknownHostException {
        check("java.util.UUID", UUID.class, "127.0.0.1", InetAddress.getByName("127.0.0.1"));
    }

    @Test(expected=loghub.ProcessorException.class)
    public void TestInvalidNumber() throws ProcessorException {
        check("java.lang.Integer", java.lang.Integer.class, "a", "");
    }

    @Test(expected=loghub.ProcessorException.class)
    public void TestBufferTooSmall() throws ProcessorException {
        check("java.lang.Double", Double.class, generate(4, b -> b.putFloat((float)38)), (double) 38);
    }

}
