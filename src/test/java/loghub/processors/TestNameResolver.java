package loghub.processors;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.configuration.Properties;

public class TestNameResolver {

    @Test
    public void test2() throws UnknownHostException {
        NameResolver nr = new NameResolver();
        nr.configure(new Properties(Collections.emptyMap()));
        nr.setField("host");
        nr.setDestination("fqdn");
        Event e = new Event();
        e.put("host", InetAddress.getByName("127.0.0.1"));
        nr.process(e);
        Assert.assertEquals("resolution failed", "localhost", e.get("fqdn_host"));
    }

    @Test
    public void test3() throws UnknownHostException {
        NameResolver nr = new NameResolver();
        nr.configure(new Properties(Collections.emptyMap()));
        nr.setResolver("dns:");
        nr.setField("host");
        nr.setDestination("fqdn_${field%s}");
        Event e = new Event();
        /// resolving a.root-servers.net.
        e.put("host", InetAddress.getByName("2001:503:ba3e::2:30"));
        nr.process(e);
        System.out.println(e);
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_host"));
    }
}
