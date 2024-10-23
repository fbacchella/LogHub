package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestCidr {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void checkIp() throws ProcessorException {
        Assert.assertEquals("fe80::", resolve("fe80::1"));
        Assert.assertEquals("2001:db8:1:1a0::", resolve("2001:db8:1:1bf:ffff:ffff:ffff:ffff"));
        Assert.assertEquals("10.0.0.0", resolve("10.1.1.1"));
        Assert.assertEquals("192.168.0.0", resolve("192.168.1.1"));
        Assert.assertEquals("172.16.0.0", resolve("172.16.0.1"));
    }

    public String resolve(String ip) throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fe80::/64", "2001:db8:1:1a0::/59"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        Cidr cidr = builder.build();
        Assert.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", ip);
        Assert.assertTrue(cidr.process(event));
        return (String) event.getMeta("network");
    }

    @Test
    public void notNetmask() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"not/an/netmask"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertEquals("Not a valid net mask: not/an/netmask", ex.getMessage());
    }

    @Test
    public void notIP() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"/256.256.256.256/8"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertTrue(ex.getMessage().startsWith("Not an IP address: /256.256.256.256: "));
    }

    @Test
    public void notFound() throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fe80::/64", "2001:db8:1:1a0::/59"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        Cidr cidr = builder.build();
        Assert.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", "127.0.0.1");
        Assert.assertFalse(cidr.process(event));
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Cidr"
                , BeanChecks.BeanInfo.build("networks", String[].class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
