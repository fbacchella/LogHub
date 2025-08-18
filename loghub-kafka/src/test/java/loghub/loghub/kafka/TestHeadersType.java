package loghub.loghub.kafka;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import loghub.kafka.HeadersTypes;

public class TestHeadersType {

    @Test
    public void testRoundRobin() {
        Random random = new Random();
        Function<Object, HeadersTypes> detect = o -> HeadersTypes.resolve(o);
        roundRobin(detect, Long.MAX_VALUE);
        roundRobin(detect, Double.MAX_VALUE);
        roundRobin(detect, Double.NaN);
        roundRobin(detect, Float.MAX_VALUE);
        roundRobin(detect, random.nextLong());
        roundRobin(detect, random.nextFloat());
        roundRobin(detect, random.nextDouble());
        roundRobin(detect, random.nextInt());
        roundRobin(detect, (short) 42);
        roundRobin(detect, (byte) 42);
        roundRobin(detect, Boolean.TRUE);
        roundRobin(detect, Boolean.FALSE);
        roundRobin(detect, null);
        roundRobin(detect, "LogHub");
        roundRobin(detect, 'l');
        roundRobin(detect, InetAddress.getLoopbackAddress());
    }

    @Test
    public void testUnknown() {
        HeadersTypes type = HeadersTypes.resolve(getClass());
        byte[] serialized = type.write(getClass());
        Object parsed = type.read(serialized);
        Assert.assertEquals(getClass().toString(), new String((byte[]) parsed, StandardCharsets.UTF_8));
    }

    private void roundRobin(Function<Object, HeadersTypes> detect, Object value) {
        HeadersTypes type = detect.apply(value);
        byte[] serialized = type.write(value);
        Object parsed = type.read(serialized);
        Assert.assertEquals(value, parsed);
    }

}
