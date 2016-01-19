package loghub.processors;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class TestNameResolver {

    @Test
    public void test1() throws UnknownHostException, ProcessorException {
        NameResolver nr = new NameResolver();
        nr.setField("host");
        nr.setDestination("fqdn");
        nr.configure(new Properties(Collections.emptyMap()));
        Event e = new Event();
        e.put("host", InetAddress.getByName("10.0.0.1"));
        nr.process(e);
        Assert.assertEquals("resolution failed", null, e.get("fqdn"));
    }

    @Test
    public void test2() throws UnknownHostException, ProcessorException {
        NameResolver nr = new NameResolver();
        nr.setField("host");
        nr.setDestination("fqdn");
        nr.configure(new Properties(Collections.emptyMap()));
        Event e = new Event();
        e.put("host", InetAddress.getByName("127.0.0.1"));
        nr.process(e);
        Assert.assertEquals("resolution failed", "localhost", e.get("fqdn"));
    }

    @Test
    public void test3() throws UnknownHostException, ProcessorException {
        NameResolver nr = new NameResolver();
        nr.setResolver("8.8.8.8");
        nr.setField("host");
        nr.setDestination("fqdn_${field%s}");
        nr.configure(new Properties(Collections.emptyMap()));
        Event e = new Event();
        /// resolving a.root-servers.net.
        e.put("host", InetAddress.getByName("2001:503:ba3e::2:30"));
        nr.process(e);
        System.out.println(e);
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_host"));
    }
}
