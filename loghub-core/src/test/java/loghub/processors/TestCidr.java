package loghub.processors;

import java.beans.IntrospectionException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

class TestCidr {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    void checkIp() throws ProcessorException {
        Assertions.assertEquals("fe80:0:0:0:0:0:0:0/64", resolve("fe80::1"));
        Assertions.assertEquals("2001:db8:1:1a0:0:0:0:0/59", resolve("2001:db8:1:1bf:ffff:ffff:ffff:ffff"));
        Assertions.assertEquals("10.0.0.0/8", resolve("10.1.1.1"));
        Assertions.assertEquals("192.168.0.0/16", resolve("192.168.1.1"));
        Assertions.assertEquals("172.16.0.0/12", resolve("172.16.0.1"));
    }

    private String resolve(String ip) throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fe80::/64", "2001:db8:1:1a0::/59"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", ip);
        Assertions.assertTrue(cidr.process(event));
        return event.getMeta("network").toString();
    }

    @Test
    void notNetmask() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"not/an/netmask"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, builder::build);
        Assertions.assertEquals("Not a valid netmask: not/an/netmask", ex.getMessage());
    }

    @Test
    void notIP() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"/256.256.256.256/8"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, builder::build);
        Assertions.assertEquals("Not a valid netmask: /256.256.256.256/8", ex.getMessage());
    }

    @Test
    void notFound() throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fe80::/64", "2001:db8:1:1a0::/59"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.ofMeta("network"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", "127.0.0.1");
        Assertions.assertFalse(cidr.process(event));
    }

    @Test
    void testDestinationTemplate() throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestinationTemplate(new VarFormatter("${field}_net"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", "192.168.1.1");
        Assertions.assertTrue(cidr.process(event));
        Assertions.assertEquals("192.168.0.0/16", event.get("ip_net").toString());
    }

    @Test
    void testInPlace() throws ProcessorException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"10.0.0.0/8"});
        builder.setField(VariablePath.of("ip"));
        builder.setInPlace(true);
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", "10.1.2.3");
        Assertions.assertTrue(cidr.process(event));
        // In-place replaces the field value with the result of fieldFunction
        Assertions.assertEquals("10.0.0.0/8", event.get("ip").toString());
    }

    @Test
    void testMultipleFields() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"192.168.0.0/16"});
        builder.setFields(new String[]{"ip1", "ip2"});
        builder.setDestinationTemplate(new VarFormatter("net_${field}"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip1", "192.168.1.1");
        event.put("ip2", "192.168.2.1");

        Tools.runProcessing(event, "main", Collections.singletonList(cidr));

        Assertions.assertEquals("192.168.0.0/16", event.get("net_ip1").toString());
        Assertions.assertEquals("192.168.0.0/16", event.get("net_ip2").toString());
    }

    @Test
    void testInetAddressInput() throws ProcessorException, UnknownHostException {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"127.0.0.0/8"});
        builder.setField(VariablePath.of("ip"));
        builder.setDestination(VariablePath.of("net"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", InetAddress.getByName("127.0.0.1"));
        Assertions.assertTrue(cidr.process(event));
        Assertions.assertEquals("127.0.0.0/8", event.get("net").toString());
    }

    @Test
    void testInvalidIpInput() {
        Cidr.Builder builder = Cidr.getBuilder();
        builder.setNetworks(new String[]{"127.0.0.0/8"});
        builder.setField(VariablePath.of("ip"));
        Cidr cidr = builder.build();
        Assertions.assertTrue(cidr.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("ip", "not.an.ip");
        ProcessorException ex = Assertions.assertThrows(ProcessorException.class, () -> cidr.process(event));
        Assertions.assertTrue(ex.getMessage().contains("Not an IP address"));
    }

    @Test
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
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
