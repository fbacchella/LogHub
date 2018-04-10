package loghub.processors;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.codec.dns.DnsRecordType;
import loghub.Event;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestNettyNameResolver {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.NettyNameResolver");
    }

    private Tools.ProcessingStatus dorequest(Consumer<NettyNameResolver> setupProc, Event e, String... warmup) throws ProcessorException {
        NettyNameResolver proc = new NettyNameResolver();
        proc.setResolver("8.8.8.8");
        setupProc.accept(proc);

        BiConsumer<Properties, List<Processor>> prepare = (i, j) -> {
            try {
                for (String name: warmup) {
                    if (name != null && ! name.isEmpty()) {
                        proc.warmUp(name, DnsRecordType.PTR);
                    }
                }
            } catch (Throwable e1) {
                throw new RuntimeException(e1);
            }
        };

        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(proc), prepare);
        return status;
    }

    @Test(timeout=2000)
    public void badresolvertimeout() throws Throwable {

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("10.0.0.1"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setResolver("169.254.1.1");
            i.setField("host");
            i.setDestination("fqdn");
            i.setTimeout(1);
        }, e);

        e = status.mainQueue.take();
        Assert.assertEquals("resolution not failed", null, e.get("fqdn"));
        // Check that the second processor executed was indeed paused
        Assert.assertEquals("resolution not paused", "PAUSED", status.status.get(2));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());

    }

    @Test(timeout=6000)
    public void badresolvernxdomain() throws Throwable {

        Event e = Tools.getEvent();
        /// resolve a no existing name
        e.put("host", InetAddress.getByName("169.254.1.1"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField("host");
            i.setDestination("fqdn");
            i.setTimeout(1);
        } , e, "1.1.254.169.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution not failed", null, e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv4addr() throws Throwable {

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField("host");
            i.setDestination("fqdn");
        } , e, "4.0.41.198.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv4string() throws ProcessorException, InterruptedException {

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv4 as String
        e.put("host", "198.41.0.4");

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField("host");
            i.setDestination("fqdn");
        } , e, "4.0.41.198.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());

    }


    @Test(timeout=6000)
    public void arootasipv6addr() throws Throwable {

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv6
        e.put("host", InetAddress.getByName("2001:503:ba3e::2:30"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField("host");
            i.setDestination("fqdn");
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());

    }

    @Test(timeout=6000)
    public void arootasipv6string() throws ProcessorException, InterruptedException, ConfigException, IOException {

        Event e = Tools.getEvent();
        // resolving a.root-servers.net. in IPv6 as a String
        e.put("host", "2001:503:ba3e::2:30");

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField("host");
            i.setDestination("fqdn");
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());

    }

    @Test(timeout=6000)
    public void resolvemany() throws ProcessorException, InterruptedException, ConfigException, IOException {

        Event e = Tools.getEvent();
        e.put("hostipv6str", "2001:503:ba3e::2:30");
        e.put("hostipv6inet",  InetAddress.getByName("2001:503:ba3e::2:30"));
        e.put("hostipv4str", "198.41.0.4");
        e.put("hostipv4inet",  InetAddress.getByName("198.41.0.4"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setFields(new String[]{"*"});
            i.setDestination("fqdn_${field}");
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa", "4.0.41.198.in-addr.ptr");

        do {
            e = status.mainQueue.element();
            Thread.sleep(10);
        } while(e.size() != 8);

        e = status.mainQueue.take();
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
        for (String i: new String[] {"hostipv6str", "hostipv6inet", "hostipv4str", "hostipv4inet"}) {
            Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_" +i));
        }
        Assert.assertEquals("Queue not empty: " + status.mainQueue.size(), 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }
}
